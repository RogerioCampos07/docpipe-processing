"""Destructive recovery checks for a disposable PostgreSQL stack."""

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
from urllib.parse import urlsplit
from urllib.request import urlopen
from uuid import uuid4

import aio_pika
import pytest
from aio_pika import DeliveryMode, Message
from sqlalchemy import text

from docpipe_processing.persistence import (
    PersistenceUnavailableError,
    ProcessingJobRepository,
    create_database_engine,
    create_session_factory,
)
from docpipe_processing.worker import (
    WorkerSettings,
    declare_processing_topology,
)
from tests.conftest import DatabaseFixture

pytestmark = [
    pytest.mark.integration,
    pytest.mark.persistence,
    pytest.mark.resilience,
    pytest.mark.skipif(
        not os.getenv('PROCESSING_POSTGRES_CONTAINER_ID'),
        reason='requires explicitly disposable PostgreSQL container',
    ),
]


def _required_environment(name: str) -> str:
    value = os.getenv(name, '').strip()
    if not value:
        pytest.skip(f'Set {name} to the disposable test container.')
    return value


def _verify_disposable_postgres(container_id: str, database_url: str) -> str:
    result = subprocess.run(
        [
            'docker',
            'inspect',
            '--format',
            '{{.Config.Image}} {{.Name}} {{.State.Status}} '
            '{{.HostConfig.AutoRemove}}',
            container_id,
        ],
        capture_output=True,
        check=True,
        text=True,
        timeout=10,
    )
    image, name, state, auto_remove = result.stdout.strip().split()
    assert image == 'postgres:17.11-bookworm'
    assert name.startswith('/docpipe-et6-pg-')
    assert state == 'running'
    assert auto_remove == 'false'
    mapped = (
        subprocess
        .run(
            ['docker', 'port', container_id, '5432/tcp'],
            capture_output=True,
            check=True,
            text=True,
            timeout=10,
        )
        .stdout.strip()
        .rsplit(':', maxsplit=1)[-1]
    )
    configured_port = urlsplit(database_url).port
    assert configured_port is not None
    assert mapped == str(configured_port), (
        'PostgreSQL recovery tests require a stable, explicitly mapped host '
        'port matching PROCESSING_TEST_DATABASE_URL.'
    )
    return mapped


def _stop_postgres(container_id: str) -> None:
    subprocess.run(
        ['docker', 'stop', '--time', '2', container_id],
        capture_output=True,
        check=True,
        timeout=15,
    )


def _start_postgres(container_id: str) -> None:
    status = subprocess.run(
        ['docker', 'inspect', '--format', '{{.State.Status}}', container_id],
        capture_output=True,
        check=True,
        text=True,
        timeout=10,
    ).stdout.strip()
    if status != 'running':
        subprocess.run(
            ['docker', 'start', container_id],
            capture_output=True,
            check=True,
            timeout=30,
        )
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        result = subprocess.run(
            [
                'docker',
                'exec',
                container_id,
                'pg_isready',
                '-U',
                'processing_test',
                '-d',
                'processing_test',
            ],
            capture_output=True,
            check=False,
            timeout=5,
        )
        if result.returncode == 0:
            return
        time.sleep(0.5)
    raise AssertionError(
        'Disposable PostgreSQL did not recover in 60 seconds.'
    )


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return int(sock.getsockname()[1])


def _settings(token: str, amqp_url: str) -> WorkerSettings:
    return WorkerSettings(
        amqp_url=amqp_url,
        exchange=f'resilience.events.{token}',
        exchange_type='direct',
        routing_key='document.received.v1',
        queue=f'processing.resilience.{token}',
        dead_letter_exchange=f'processing.resilience.dlx.{token}',
        dead_letter_queue=f'processing.resilience.dead.{token}',
        dead_letter_routing_key='processing.rejected.v1',
        metrics_host='127.0.0.1',
        metrics_port=_free_port(),
        heartbeat_seconds=10,
    )


def _payload(event_id: str) -> bytes:
    return json.dumps({
        'event_id': event_id,
        'event_type': 'document.received',
        'event_version': 1,
        'occurred_at': '2026-01-01T00:00:00Z',
        'correlation_id': None,
        'document_id': f'doc-{event_id}',
        'data': {
            'storage_key': f'key-{event_id}',
            'media_type': 'application/pdf',
            'size_bytes': 0,
            'sha256': 'a' * 64,
        },
    }).encode()


def _worker_environment(
    database: DatabaseFixture, settings: WorkerSettings
) -> dict[str, str]:
    return {
        **os.environ,
        'PROCESSING_DATABASE_URL': database.url,
        'PROCESSING_AMQP_URL': settings.amqp_url,
        'PROCESSING_AMQP_EXCHANGE': settings.exchange,
        'PROCESSING_AMQP_EXCHANGE_TYPE': settings.exchange_type,
        'PROCESSING_AMQP_ROUTING_KEY': settings.routing_key,
        'PROCESSING_AMQP_QUEUE': settings.queue,
        'PROCESSING_AMQP_DLX': settings.dead_letter_exchange,
        'PROCESSING_AMQP_DLQ': settings.dead_letter_queue,
        'PROCESSING_AMQP_DLQ_ROUTING_KEY': settings.dead_letter_routing_key,
        'PROCESSING_WORKER_METRICS_HOST': settings.metrics_host,
        'PROCESSING_WORKER_METRICS_PORT': str(settings.metrics_port),
        'PROCESSING_AMQP_HEARTBEAT_SECONDS': str(settings.heartbeat_seconds),
    }


def _worker_counter(port: int, metric_name: str) -> float:
    with urlopen(f'http://127.0.0.1:{port}/metrics', timeout=3) as response:
        lines = response.read().decode().splitlines()
    for line in lines:
        if line.startswith(f'{metric_name} '):
            return float(line.rsplit(' ', maxsplit=1)[-1])
    raise AssertionError(f'Worker metric {metric_name} was not exposed.')


async def _wait_for_worker_counter(port: int, metric_name: str) -> float:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            return await asyncio.to_thread(_worker_counter, port, metric_name)
        except OSError, TimeoutError:
            await asyncio.sleep(0.1)
    raise AssertionError('Worker metrics endpoint did not become available.')


async def _wait_for_job(
    repository: ProcessingJobRepository, event_id: str, timeout: float = 45
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            job = await asyncio.to_thread(
                repository.get_by_source_event_id, event_id
            )
        except PersistenceUnavailableError:
            await asyncio.sleep(0.25)
            continue
        if job is not None:
            accepted = await asyncio.to_thread(
                repository.get_acceptance_by_source_event_id, event_id
            )
            assert accepted is not None
            assert accepted[1] is not None
            assert accepted[0] == job
            return True
        await asyncio.sleep(0.25)
    return False


async def _prepare_source_exchange(
    channel: aio_pika.abc.AbstractChannel, settings: WorkerSettings
) -> None:
    """Provision only the test producer's exchange before worker startup."""
    await channel.declare_exchange(
        settings.exchange,
        type=settings.exchange_type,
        durable=True,
    )


async def _cleanup_topology(settings: WorkerSettings) -> None:
    connection = await aio_pika.connect(settings.amqp_url, timeout=5)
    try:
        channel = await connection.channel(publisher_confirms=False)
        for name in (settings.queue, settings.dead_letter_queue):
            await channel.queue_delete(name)
        for name in (settings.exchange, settings.dead_letter_exchange):
            await channel.exchange_delete(name)
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_worker_restarts_and_recovers_after_prolonged_postgres_outage(
    database: DatabaseFixture,
) -> None:
    postgres_id = _required_environment('PROCESSING_POSTGRES_CONTAINER_ID')
    amqp_url = _required_environment('PROCESSING_AMQP_URL')
    postgres_port = _verify_disposable_postgres(postgres_id, database.url)
    token = uuid4().hex
    event_id = f'resilience-{token}'
    settings = _settings(token, amqp_url)
    engine = create_database_engine(database.url)
    repository = ProcessingJobRepository(create_session_factory(engine))
    connection = await aio_pika.connect(amqp_url, timeout=5)
    channel = await connection.channel()
    worker: subprocess.Popen[bytes] | None = None
    try:
        await _prepare_source_exchange(channel, settings)
        await declare_processing_topology(channel, settings)
        exchange = await channel.get_exchange(settings.exchange)
        _stop_postgres(postgres_id)
        await exchange.publish(
            Message(
                _payload(event_id),
                content_type='application/json',
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=event_id,
            ),
            routing_key=settings.routing_key,
            mandatory=True,
        )

        # Both worker generations run longer than the local retry window while
        # the real PostgreSQL container is stopped. Startup probes prevent AMQP
        # consumption until the database returns.
        for _ in range(2):
            worker = subprocess.Popen(
                [sys.executable, '-m', 'docpipe_processing.worker'],
                env=_worker_environment(database, settings),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            await asyncio.sleep(7)
            assert worker.poll() is None
            declaration = await channel.declare_queue(
                settings.queue, durable=True, passive=True
            )
            dead = await channel.declare_queue(
                settings.dead_letter_queue, durable=True, passive=True
            )
            assert declaration.declaration_result.message_count == 1
            assert dead.declaration_result.message_count == 0
            assert (
                await _wait_for_worker_counter(
                    settings.metrics_port, 'docpipe_worker_ack_sent_total'
                )
                == 0
            )
            worker.terminate()
            await asyncio.to_thread(worker.wait, 10)
            worker = None

        _start_postgres(postgres_id)
        assert _verify_disposable_postgres(postgres_id, database.url) == (
            postgres_port
        )
        worker = subprocess.Popen(
            [sys.executable, '-m', 'docpipe_processing.worker'],
            env=_worker_environment(database, settings),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        assert await _wait_for_job(repository, event_id)
        declaration = await channel.declare_queue(
            settings.queue, durable=True, passive=True
        )
        dead = await channel.declare_queue(
            settings.dead_letter_queue, durable=True, passive=True
        )
        assert declaration.declaration_result.message_count == 0
        assert dead.declaration_result.message_count == 0
        assert repository.get_by_source_event_id(event_id) is not None
    finally:
        _start_postgres(postgres_id)
        if worker is not None and worker.poll() is None:
            worker.terminate()
            await asyncio.to_thread(worker.wait, 10)
        await _cleanup_topology(settings)
        await connection.close()
        engine.dispose()


@pytest.mark.asyncio
async def test_database_interrupt_during_active_insert_is_recovered(  # noqa: PLR0914, PLR0915
    database: DatabaseFixture,
) -> None:
    """Stop PostgreSQL only after a test-local trigger observes an INSERT."""
    postgres_id = _required_environment('PROCESSING_POSTGRES_CONTAINER_ID')
    amqp_url = _required_environment('PROCESSING_AMQP_URL')
    postgres_port = _verify_disposable_postgres(postgres_id, database.url)
    token = uuid4().hex
    event_id = f'active-insert-{token}'
    settings = _settings(token, amqp_url)
    engine = create_database_engine(database.url)
    repository = ProcessingJobRepository(create_session_factory(engine))
    worker: subprocess.Popen[bytes] | None = None
    connection = await aio_pika.connect(amqp_url, timeout=5)
    channel = await connection.channel()
    try:
        await _prepare_source_exchange(channel, settings)
        await declare_processing_topology(channel, settings)
        exchange = await channel.get_exchange(settings.exchange)
        with engine.begin() as db_connection:
            db_connection.execute(
                text(
                    'CREATE FUNCTION resilience_pause_insert() '
                    'RETURNS trigger '
                    'LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(20); '
                    'RETURN NEW; END $$'
                )
            )
            db_connection.execute(
                text(
                    'CREATE TRIGGER resilience_pause_insert BEFORE INSERT ON '
                    'processing_jobs FOR EACH ROW EXECUTE FUNCTION '
                    'resilience_pause_insert()'
                )
            )

        worker = subprocess.Popen(
            [sys.executable, '-m', 'docpipe_processing.worker'],
            env=_worker_environment(database, settings),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            declaration = await channel.declare_queue(
                settings.queue, durable=True, passive=True
            )
            if (declaration.declaration_result.consumer_count or 0) > 0:
                break
            await asyncio.sleep(0.2)
        else:
            raise AssertionError(
                'Worker did not subscribe to the input queue.'
            )

        await exchange.publish(
            Message(
                _payload(event_id),
                content_type='application/json',
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=event_id,
            ),
            routing_key=settings.routing_key,
            mandatory=True,
        )

        active_insert = False
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            with engine.connect() as db_connection:
                active_insert = bool(
                    db_connection.scalar(
                        text(
                            'SELECT EXISTS (SELECT 1 FROM pg_stat_activity '
                            'WHERE '
                            "query LIKE 'INSERT INTO processing_jobs%' AND "
                            "state = 'active')"
                        )
                    )
                )
            if active_insert:
                break
            await asyncio.sleep(0.1)
        assert active_insert, 'PostgreSQL did not observe the active INSERT.'
        assert (
            _worker_counter(
                settings.metrics_port, 'docpipe_worker_deliveries_total'
            )
            == 1
        )
        assert (
            _worker_counter(
                settings.metrics_port, 'docpipe_worker_ack_sent_total'
            )
            == 0
        )
        _stop_postgres(postgres_id)
        await asyncio.sleep(3)
        assert (
            _worker_counter(
                settings.metrics_port, 'docpipe_worker_ack_sent_total'
            )
            == 0
        )

        # Recover while the callback retains its unacknowledged delivery.
        _start_postgres(postgres_id)
        assert _verify_disposable_postgres(postgres_id, database.url) == (
            postgres_port
        )
        assert await _wait_for_job(repository, event_id)
        declaration = await channel.declare_queue(
            settings.queue, durable=True, passive=True
        )
        dead = await channel.declare_queue(
            settings.dead_letter_queue, durable=True, passive=True
        )
        assert declaration.declaration_result.message_count == 0
        assert dead.declaration_result.message_count == 0
        assert (
            repository.get_acceptance_by_source_event_id(event_id) is not None
        )
    finally:
        _start_postgres(postgres_id)
        if worker is not None and worker.poll() is None:
            worker.terminate()
            await asyncio.to_thread(worker.wait, 10)
        await _cleanup_topology(settings)
        await connection.close()
        engine.dispose()
