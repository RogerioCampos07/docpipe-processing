"""HTTP queries over committed, Processing-owned PostgreSQL data."""

import logging
from dataclasses import asdict
from http import HTTPStatus
from pathlib import Path
from typing import Iterator
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import insert, text

from docpipe_processing.app import app
from docpipe_processing.contracts import DocumentReceivedEvent
from docpipe_processing.domain import ProcessingStatus
from docpipe_processing.persistence import (
    DATABASE_URL_ENV_VAR,
    ProcessingJobModel,
    ProcessingJobRepository,
    create_database_engine,
    create_session_factory,
)
from docpipe_processing.processing_input import ProcessingInput
from tests.conftest import DatabaseFixture
from tests.test_api import PATH, PRIVATE_REASON, PUBLIC_FIELDS, snapshot

pytestmark = [pytest.mark.integration, pytest.mark.persistence]


@pytest.fixture(autouse=True)
def preserve_http_logging() -> Iterator[None]:
    # Alembic configures logging in-process in the database fixture. Preserve
    # the pre-migration state so these tests do not disable later HTTP logs.
    loggers = [
        logging.getLogger(name)
        for name in ('docpipe_processing.http', 'docpipe_processing.api')
    ]
    disabled = [logger.disabled for logger in loggers]
    try:
        yield
    finally:
        for logger, previous in zip(loggers, disabled, strict=True):
            logger.disabled = previous


@pytest.fixture
def repository(database: DatabaseFixture) -> Iterator[ProcessingJobRepository]:
    engine = create_database_engine(database.url)
    try:
        yield ProcessingJobRepository(create_session_factory(engine))
    finally:
        engine.dispose()


@pytest.fixture
def http(
    database: DatabaseFixture, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    monkeypatch.setenv(DATABASE_URL_ENV_VAR, database.url)
    for name in ('docpipe_processing.http', 'docpipe_processing.api'):
        monkeypatch.setattr(logging.getLogger(name), 'disabled', False)
    with TestClient(app) as client:
        yield client


def synthetic_input() -> ProcessingInput:
    fixture = Path(__file__).parent / 'fixtures' / 'document_received_v1.json'
    event = DocumentReceivedEvent.model_validate_json(fixture.read_text())
    return ProcessingInput.from_event(event)


@pytest.mark.parametrize('status', list(ProcessingStatus))
def test_http_reads_committed_snapshot_without_mutating_job_or_input(
    repository: ProcessingJobRepository,
    http: TestClient,
    status: ProcessingStatus,
) -> None:
    job = snapshot(status)
    input = synthetic_input()
    repository.create(snapshot(), input=input)
    if status is not ProcessingStatus.RECEIVED:
        repository.save(job)

    for _ in range(2):
        response = http.get(PATH)
        assert response.status_code == HTTPStatus.OK
        body = response.json()
        assert set(body) == PUBLIC_FIELDS
        assert body['status'] == status.value
        assert body['attempts'] == job.attempts
        assert body['correlation_id'] is None
        assert body['failure_reason'] == (
            'O processamento falhou.'
            if status is ProcessingStatus.FAILED
            else None
        )
        assert PRIVATE_REASON not in response.text
        assert input.storage_key not in response.text
    assert repository.get_by_processing_id(job.processing_id) == job
    assert repository.get_input_by_processing_id(job.processing_id) == input


def test_legacy_job_is_queryable_without_inventing_durable_input(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
    http: TestClient,
) -> None:
    job = snapshot()
    engine = create_database_engine(database.url)
    try:
        with engine.begin() as connection:
            connection.execute(
                insert(ProcessingJobModel).values(**asdict(job))
            )
        response = http.get(PATH)
        assert response.status_code == HTTPStatus.OK
        assert set(response.json()) == PUBLIC_FIELDS
        assert repository.get_by_processing_id(job.processing_id) == job
        assert repository.get_input_by_processing_id(job.processing_id) is None
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text('SELECT count(*) FROM processing_jobs')
                ).scalar_one()
                == 1
            )
    finally:
        engine.dispose()


def test_missing_persisted_job_returns_404(http: TestClient) -> None:
    response = http.get(f'/v1/processing-jobs/{UUID(int=0)}')
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['code'] == 'processing_job_not_found'


@pytest.mark.parametrize(
    'statement',
    [
        "UPDATE processing_jobs SET status = 'COMPLETED'",
        "UPDATE processing_jobs SET status = 'UNKNOWN'",
        'ALTER TABLE processing_jobs RENAME TO incompatible_jobs',
    ],
)
def test_invalid_data_or_schema_returns_safe_500_and_preserves_liveness(
    database: DatabaseFixture,
    repository: ProcessingJobRepository,
    http: TestClient,
    statement: str,
) -> None:
    repository.create(snapshot(), input=synthetic_input())
    engine = create_database_engine(database.url)
    try:
        with engine.begin() as connection:
            connection.execute(text(statement))
        response = http.get(PATH)
        assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
        assert response.json() == {
            'code': 'internal_error',
            'message': 'Não foi possível concluir a consulta.',
        }
        assert 'processing_jobs' not in response.text
        assert database.url not in response.text
        assert http.get('/health').json() == {'status': 'ok'}
        assert http.get('/metrics').status_code == HTTPStatus.OK
    finally:
        engine.dispose()
