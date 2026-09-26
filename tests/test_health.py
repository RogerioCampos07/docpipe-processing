from http import HTTPStatus

from fastapi.testclient import TestClient

from docpipe_processing.app import app


def test_health_returns_ok() -> None:
    with TestClient(app) as client:
        response = client.get('/health')

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'status': 'ok'}
