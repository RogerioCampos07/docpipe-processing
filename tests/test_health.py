import json
import logging
from http import HTTPStatus

import pytest
from fastapi.testclient import TestClient

from docpipe_processing.app import app
from docpipe_processing.observability import SERVICE_NAME, JsonFormatter


def _structured_event(record: logging.LogRecord) -> dict[str, object]:
    raw_event: object = record.__dict__.get('structured_event')
    assert isinstance(raw_event, dict)
    event: dict[str, object] = {
        key: value for key, value in raw_event.items() if isinstance(key, str)
    }
    return event


def test_health_returns_ok() -> None:
    with TestClient(app) as client:
        response = client.get('/health')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'status': 'ok'}


def test_metrics_endpoint_exposes_http_request_metrics() -> None:
    with TestClient(app) as client:
        client.get('/health')
        response = client.get('/metrics', follow_redirects=False)

    assert response.status_code == HTTPStatus.OK
    assert (
        'docpipe_http_requests_total{method="GET",route="/health",status_code="200"}'
        in response.text
    )


def test_health_request_emits_structured_operational_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger='docpipe_processing.http')

    with TestClient(app) as client:
        client.get('/health')

    record = next(
        record
        for record in caplog.records
        if record.name == 'docpipe_processing.http'
    )

    event = _structured_event(record)
    assert event['service'] == SERVICE_NAME
    assert event['operation'] == 'http.request'
    assert event['method'] == 'GET'
    assert event['route'] == '/health'
    assert event['status_code'] == HTTPStatus.OK
    assert event['state'] == 'completed'
    duration = event['duration_seconds']
    assert isinstance(duration, (int, float))
    assert duration >= 0


def test_health_request_extracts_w3c_trace_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    trace_id = '0123456789abcdef0123456789abcdef'
    caplog.set_level(logging.INFO, logger='docpipe_processing.http')

    with TestClient(app) as client:
        response = client.get(
            '/health',
            headers={'traceparent': f'00-{trace_id}-0123456789abcdef-01'},
        )

    record = next(
        record
        for record in caplog.records
        if record.name == 'docpipe_processing.http'
    )

    assert response.status_code == HTTPStatus.OK
    assert _structured_event(record)['trace_id'] == trace_id


def test_json_formatter_serializes_operational_fields() -> None:
    record = logging.LogRecord(
        name='docpipe_processing.http',
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg='http_request',
        args=(),
        exc_info=None,
    )
    record.structured_event = {
        'operation': 'http.request',
        'state': 'completed',
        'duration_seconds': 0.01,
    }

    payload = json.loads(JsonFormatter().format(record))

    assert payload == {
        'service': SERVICE_NAME,
        'severity': 'info',
        'operation': 'http.request',
        'state': 'completed',
        'duration_seconds': 0.01,
    }
