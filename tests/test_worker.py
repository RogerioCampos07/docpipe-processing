"""Worker configuration and topology declarations stay local and explicit."""

import asyncio
from types import SimpleNamespace
from typing import Any, cast

import aio_pika
import pytest
from aio_pika import ExchangeType
from prometheus_client import CollectorRegistry

from docpipe_processing import worker
from docpipe_processing.consumer import ConsumerMetrics
from docpipe_processing.persistence import PersistenceUnavailableError
from docpipe_processing.worker import (
    WorkerConfigurationError,
    WorkerFatalError,
    WorkerSettings,
    declare_processing_topology,
    input_queue_arguments,
)

pytestmark = pytest.mark.unit


def _settings() -> WorkerSettings:
    return WorkerSettings(
        amqp_url='amqp://worker@localhost/processing',
        exchange='docpipe.events',
        exchange_type='direct',
        routing_key='document.received.v1',
        queue='processing.document-received.v1',
        dead_letter_exchange='processing.dlx.v1',
        dead_letter_queue='processing.dead-letter.v1',
        dead_letter_routing_key='processing.rejected.v1',
        metrics_host='127.0.0.1',
        metrics_port=9000,
        heartbeat_seconds=30,
    )


def test_worker_settings_use_observed_public_exchange_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        'PROCESSING_AMQP_URL',
        'amqp://worker:synthetic-password@localhost:5672/processing',
    )

    settings = WorkerSettings.from_environment()

    assert settings.exchange == 'docpipe.events'
    assert settings.exchange_type == 'direct'
    assert settings.routing_key == 'document.received.v1'
    assert settings.queue == 'processing.document-received.v1'
    assert settings.dead_letter_exchange == 'processing.dlx.v1'
    assert settings.dead_letter_queue == 'processing.dead-letter.v1'
    assert settings.dead_letter_routing_key == 'processing.rejected.v1'


@pytest.mark.parametrize(
    ('key', 'value'),
    [
        ('PROCESSING_AMQP_EXCHANGE_TYPE', 'invalid'),
        ('PROCESSING_WORKER_METRICS_PORT', 'not-a-port'),
        ('PROCESSING_AMQP_HEARTBEAT_SECONDS', '0'),
    ],
)
def test_worker_rejects_invalid_configuration(
    monkeypatch: pytest.MonkeyPatch,
    key: str,
    value: str,
) -> None:
    monkeypatch.setenv(
        'PROCESSING_AMQP_URL',
        'amqp://worker:synthetic-password@localhost:5672/processing',
    )
    monkeypatch.setenv(key, value)

    with pytest.raises(WorkerConfigurationError):
        WorkerSettings.from_environment()


def test_worker_requires_its_own_amqp_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv('PROCESSING_AMQP_URL', raising=False)

    with pytest.raises(WorkerConfigurationError):
        WorkerSettings.from_environment()


def test_input_queue_disables_automatic_delivery_limit() -> None:
    settings = _settings()

    assert input_queue_arguments(settings) == {
        'x-queue-type': 'quorum',
        'x-dead-letter-exchange': 'processing.dlx.v1',
        'x-dead-letter-routing-key': 'processing.rejected.v1',
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
        'x-delivery-limit': -1,
    }


class FakeQueue:
    def __init__(self, name: str) -> None:
        self.name = name
        self.bindings: list[tuple[object, str | None]] = []

    async def bind(
        self, exchange: object, *, routing_key: str | None = None
    ) -> None:
        self.bindings.append((exchange, routing_key))


class FakeChannel:
    def __init__(self) -> None:
        self.declarations: list[tuple[str, dict[str, object]]] = []

    async def declare_exchange(
        self,
        name: str,
        *,
        type: ExchangeType,
        durable: bool,
        passive: bool = False,
    ) -> FakeQueue:
        self.declarations.append((
            'exchange',
            {
                'name': name,
                'type': type,
                'durable': durable,
                'passive': passive,
            },
        ))
        return FakeQueue(name)

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool,
        arguments: dict[str, object],
    ) -> FakeQueue:
        self.declarations.append((
            'queue',
            {'name': name, 'durable': durable, 'arguments': arguments},
        ))
        return FakeQueue(name)


def test_topology_passively_checks_external_exchange_and_owns_queues() -> None:
    settings = _settings()
    channel = FakeChannel()

    queue = asyncio.run(
        declare_processing_topology(
            cast(aio_pika.abc.AbstractChannel, channel), settings
        )
    )

    assert queue.name == settings.queue
    assert channel.declarations[0] == (
        'exchange',
        {
            'name': settings.exchange,
            'type': ExchangeType.DIRECT,
            'durable': True,
            'passive': True,
        },
    )
    assert channel.declarations[1][1]['durable'] is True
    assert channel.declarations[2][1]['arguments'] == {
        'x-queue-type': 'quorum'
    }
    assert channel.declarations[3][1]['arguments'] == input_queue_arguments(
        settings
    )


def test_database_probe_reports_available_and_unavailable() -> None:
    class Repository:
        unavailable = False

        def get_by_processing_id(self, _processing_id: object) -> None:
            if self.unavailable:
                raise PersistenceUnavailableError('private database detail')

    repository = Repository()
    assert asyncio.run(worker._database_probe(cast(Any, repository)))
    repository.unavailable = True
    assert not asyncio.run(worker._database_probe(cast(Any, repository)))


def test_database_wait_returns_when_ready_or_stopping(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def ready(_repository: object) -> bool:
        return True

    monkeypatch.setattr(worker, '_database_probe', ready)
    stop = asyncio.Event()
    assert asyncio.run(worker._wait_for_database(cast(Any, object()), stop))

    stop.set()
    assert not asyncio.run(
        worker._wait_for_database(cast(Any, object()), stop)
    )


class FakeRuntimeQueue:
    @staticmethod
    async def consume(
        _callback: Any,
        *,
        no_ack: bool,
        consumer_tag: str,
    ) -> str:
        assert no_ack is False
        return consumer_tag

    @staticmethod
    async def cancel(_consumer_tag: str) -> None:
        return None


class FakeRuntimeChannel:
    is_closed = False

    @staticmethod
    async def set_qos(*, prefetch_count: int) -> None:
        assert prefetch_count == 1

    @staticmethod
    async def closed() -> None:
        await asyncio.Future()

    async def close(self) -> None:
        self.is_closed = True


class FakeRuntimeConnection:
    is_closed = False

    def __init__(self) -> None:
        self.runtime_channel = FakeRuntimeChannel()

    async def channel(self, *, publisher_confirms: bool) -> FakeRuntimeChannel:
        assert publisher_confirms is False
        return self.runtime_channel

    @staticmethod
    async def closed() -> None:
        await asyncio.Future()

    async def close(self) -> None:
        self.is_closed = True


def test_serve_connection_starts_manual_consumer_and_shuts_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    queue = FakeRuntimeQueue()
    connection = FakeRuntimeConnection()
    stop = asyncio.Event()
    stop.set()

    async def connect(*_args: Any, **_kwargs: Any) -> FakeRuntimeConnection:
        return connection

    async def declare_topology(
        _channel: aio_pika.abc.AbstractChannel,
        _settings: WorkerSettings,
    ) -> FakeRuntimeQueue:
        return queue

    monkeypatch.setattr(aio_pika, 'connect', connect)
    monkeypatch.setattr(
        worker, 'declare_processing_topology', declare_topology
    )
    acceptance = cast(Any, object())
    repository = cast(Any, object())
    metrics = ConsumerMetrics(CollectorRegistry())

    asyncio.run(
        worker._serve_connection(
            settings,
            repository,
            acceptance,
            metrics,
            stop,
        )
    )

    assert connection.is_closed
    assert connection.runtime_channel.is_closed


def test_run_worker_disposes_engine_and_metrics_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _settings()
    monkeypatch.setattr(
        WorkerSettings,
        'from_environment',
        classmethod(lambda _cls: settings),
    )
    engine = SimpleNamespace(disposed=False)
    engine.dispose = lambda: setattr(engine, 'disposed', True)
    server = SimpleNamespace(closed=False)
    server.shutdown = lambda: setattr(server, 'closed', True)
    server.server_close = lambda: setattr(server, 'closed', True)
    monkeypatch.setattr(worker, 'create_database_engine', lambda: engine)
    monkeypatch.setattr(
        worker, 'create_session_factory', lambda _engine: object()
    )
    monkeypatch.setattr(
        worker, 'start_http_server', lambda **_kwargs: (server, None)
    )
    stop = asyncio.Event()
    stop.set()

    asyncio.run(worker.run_worker(stop))

    assert engine.disposed
    assert server.closed


def test_worker_main_exits_after_fatal_worker_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fail(_stop: asyncio.Event) -> None:
        raise WorkerFatalError('internal')

    monkeypatch.setattr(worker, 'configure_logging', lambda: None)
    monkeypatch.setattr(worker, 'shutdown_tracing', lambda: None)
    monkeypatch.setattr(worker, 'run_worker', fail)

    with pytest.raises(SystemExit) as raised:
        worker.main()

    assert raised.value.code == 1
