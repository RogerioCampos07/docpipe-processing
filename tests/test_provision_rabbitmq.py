"""RabbitMQ user provisioning stays scoped and opt-in."""

import asyncio
from typing import Any

import aio_pika
import pytest

from docpipe_processing import provision_rabbitmq

pytestmark = pytest.mark.unit
EXPECTED_PROVISIONING_REQUESTS = 2
MANAGEMENT_TIMEOUT_SECONDS = 10


def _provision_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        'PROCESSING_AMQP_ADMIN_HTTP_URL',
        'http://admin:synthetic-admin-password@localhost:15672',
    )
    monkeypatch.setenv(
        'PROCESSING_AMQP_ADMIN_URL',
        'amqp://admin:synthetic-admin-password@localhost:5672/processing',
    )
    monkeypatch.setenv('PROCESSING_AMQP_WORKER_USER', 'processing_worker')
    monkeypatch.setenv(
        'PROCESSING_AMQP_WORKER_PASSWORD', 'synthetic-worker-password'
    )


def test_admin_url_parsing_and_resource_patterns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _provision_environment(monkeypatch)
    base_url, credentials = provision_rabbitmq._admin_base_and_auth()

    assert base_url == 'http://localhost:15672'
    assert credentials == ('admin', 'synthetic-admin-password')
    assert provision_rabbitmq._resource_pattern(
        'input.queue', 'dead.queue'
    ) == ('^(input\\.queue|dead\\.queue)$')


def test_provision_creates_worker_user_with_scoped_permissions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _provision_environment(monkeypatch)
    calls: list[tuple[str, dict[str, object]]] = []

    def capture(
        _base: str,
        _credentials: tuple[str, str],
        path: str,
        body: dict[str, object],
    ) -> None:
        calls.append((path, body))

    monkeypatch.setattr(provision_rabbitmq, '_management_put', capture)

    asyncio.run(provision_rabbitmq.provision())

    assert calls[0][0] == 'users/processing_worker'
    assert calls[1][0] == 'permissions/processing/processing_worker'
    permissions = calls[1][1]
    assert permissions['configure'] == provision_rabbitmq._resource_pattern(
        'processing.document-received.v1',
        'processing.dlx.v1',
        'processing.dead-letter.v1',
    )
    assert 'docpipe\\.events' in str(permissions['write'])
    assert permissions['read'] == provision_rabbitmq._resource_pattern(
        'docpipe.events', 'processing.document-received.v1'
    )


def test_input_exchange_declaration_requires_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _provision_environment(monkeypatch)
    calls = 0

    def capture(
        _base: str,
        _credentials: tuple[str, str],
        _path: str,
        _body: dict[str, object],
    ) -> None:
        nonlocal calls
        calls += 1

    monkeypatch.setattr(provision_rabbitmq, '_management_put', capture)
    monkeypatch.delenv(
        'PROCESSING_AMQP_PROVISION_INPUT_EXCHANGE', raising=False
    )

    asyncio.run(provision_rabbitmq.provision())

    assert calls == EXPECTED_PROVISIONING_REQUESTS


def test_explicit_isolated_exchange_provisioning_uses_admin_connection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _provision_environment(monkeypatch)
    monkeypatch.setenv('PROCESSING_AMQP_PROVISION_INPUT_EXCHANGE', 'true')
    monkeypatch.setattr(provision_rabbitmq, '_management_put', lambda *_: None)
    declarations: list[tuple[str, aio_pika.ExchangeType, bool]] = []

    class Channel:
        @staticmethod
        async def declare_exchange(
            name: str,
            *,
            type: aio_pika.ExchangeType,
            durable: bool,
        ) -> None:
            declarations.append((name, type, durable))

    class Connection:
        is_closed = False

        @staticmethod
        async def channel(*, publisher_confirms: bool) -> Channel:
            assert publisher_confirms is False
            return Channel()

        async def close(self) -> None:
            self.is_closed = True

    async def fake_connect(url: str, *, timeout: float) -> Connection:
        assert 'admin:synthetic-admin-password' in url
        assert timeout == MANAGEMENT_TIMEOUT_SECONDS
        return Connection()

    monkeypatch.setattr(aio_pika, 'connect', fake_connect)

    asyncio.run(provision_rabbitmq.provision())

    assert declarations == [
        ('docpipe.events', aio_pika.ExchangeType.DIRECT, True)
    ]


def test_management_errors_have_no_connection_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail(*_args: Any, **_kwargs: Any) -> Any:
        raise TimeoutError('private management URL and password')

    monkeypatch.setattr(provision_rabbitmq, 'urlopen', fail)

    with pytest.raises(provision_rabbitmq.ProvisioningError) as raised:
        provision_rabbitmq._management_put(
            'http://localhost:15672',
            ('admin', 'synthetic-password'),
            'users/worker',
            {'password': 'synthetic-password'},
        )

    assert 'private' not in str(raised.value)
    assert 'synthetic-password' not in str(raised.value)
