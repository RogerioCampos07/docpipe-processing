"""RabbitMQ and PostgreSQL integration tests with synthetic events."""

import asyncio
import json
import os
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import uuid4

import aio_pika
import pytest
from aio_pika import DeliveryMode, Message
from prometheus_client import CollectorRegistry

from docpipe_processing.acceptance import AcceptedEvent, EventAcceptance
from docpipe_processing.consumer import (
    ConsumerMetrics,
    ConsumerOperations,
    EventConsumer,
    RetryPolicy,
)
from docpipe_processing.domain import ProcessingJob
from docpipe_processing.persistence import (
    ProcessingJobRepository,
    create_database_engine,
    create_session_factory,
)
from docpipe_processing.processing_input import ProcessingInput
from docpipe_processing.worker import (
    WorkerSettings,
    declare_processing_topology,
)
from tests.conftest import DatabaseFixture

pytestmark = [pytest.mark.integration, pytest.mark.consumer]


@dataclass
class ConsumerHarness:
    admin_channel: aio_pika.abc.AbstractChannel
    consumer_channel: aio_pika.abc.AbstractChannel
    consumer: EventConsumer
    queue: aio_pika.abc.AbstractQueue
    consumer_tag: str
    exchange: aio_pika.abc.AbstractExchange
    quarantine_prepared: asyncio.Event


@dataclass
class ConsumerSetup:
    admin_channel: aio_pika.abc.AbstractChannel
    consumer_channel: aio_pika.abc.AbstractChannel
    publisher_channel: aio_pika.abc.AbstractChannel
    repository: ProcessingJobRepository
    metrics: ConsumerMetrics
    settings: WorkerSettings
    stop_event: asyncio.Event
    token: str
    acceptance: EventAcceptance | None = None


async def _start_test_consumer(
    setup: ConsumerSetup,
) -> ConsumerHarness:
    admin_channel = setup.admin_channel
    consumer_channel = setup.consumer_channel
    settings = setup.settings
    quarantine_prepared = asyncio.Event()
    await admin_channel.declare_exchange(
        settings.exchange,
        type=settings.exchange_type,
        durable=True,
    )
    await declare_processing_topology(admin_channel, settings)
    await consumer_channel.set_qos(prefetch_count=1)

    async def probe() -> bool:
        await asyncio.to_thread(setup.repository.get_by_processing_id, uuid4())
        return True

    async def prepare_quarantine() -> None:
        await declare_processing_topology(admin_channel, settings)
        quarantine_prepared.set()

    consumer = EventConsumer(
        setup.acceptance or EventAcceptance(setup.repository),
        ConsumerOperations(probe, prepare_quarantine),
        setup.metrics,
        stop_event=setup.stop_event,
        retry_policy=RetryPolicy(
            transient_delays=(0.01, 0.02),
            recovery_delays=(0.05,),
        ),
    )
    queue = await declare_processing_topology(consumer_channel, settings)
    consumer_tag = await queue.consume(
        consumer.handle,
        no_ack=False,
        consumer_tag=f'processing-test-{setup.token}',
    )
    exchange = await setup.publisher_channel.get_exchange(settings.exchange)
    return ConsumerHarness(
        admin_channel,
        consumer_channel,
        consumer,
        queue,
        consumer_tag,
        exchange,
        quarantine_prepared,
    )


class CommitThenPauseAcceptance(EventAcceptance):
    """Expose the persisted-before-ACK window to a broker integration test."""

    def __init__(self, repository: ProcessingJobRepository) -> None:
        super().__init__(repository)
        self.committed = threading.Event()
        self.resume = threading.Event()

    def accept(self, body: bytes) -> AcceptedEvent:
        result = super().accept(body)
        self.committed.set()
        if not self.resume.wait(timeout=10):
            raise TimeoutError('Test did not release committed delivery.')
        return result


def _amqp_url() -> str:
    value = os.getenv('PROCESSING_AMQP_URL', '')
    if not value:
        pytest.skip('Set PROCESSING_AMQP_URL to an isolated RabbitMQ vhost.')
    return value


def _payload(event_id: str, *, sha256: str = 'a' * 64) -> bytes:
    return json.dumps({
        'event_id': event_id,
        'event_type': 'document.received',
        'event_version': 1,
        'occurred_at': '2026-01-01T12:00:00Z',
        'correlation_id': None,
        'document_id': 'synthetic-document',
        'data': {
            'storage_key': 'synthetic-object-key',
            'media_type': 'application/pdf',
            'size_bytes': 0,
            'sha256': sha256,
        },
    }).encode()


def _settings(token: str) -> WorkerSettings:
    return WorkerSettings(
        amqp_url=_amqp_url(),
        exchange=f'test.docpipe.events.{token}',
        exchange_type='direct',
        routing_key='document.received.v1',
        queue=f'processing.document-received.{token}',
        dead_letter_exchange=f'processing.dlx.{token}',
        dead_letter_queue=f'processing.dead-letter.{token}',
        dead_letter_routing_key=f'processing.rejected.{token}',
        metrics_host='127.0.0.1',
        metrics_port=9000,
        heartbeat_seconds=30,
    )


async def _wait_until(
    predicate: Callable[[], Awaitable[bool]], *, timeout: float = 10
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(0.05)
    return False


async def _publish_event(
    exchange: aio_pika.abc.AbstractExchange,
    settings: WorkerSettings,
    event_id: str,
    *,
    sha256: str = 'a' * 64,
    occurred_at: bytes = b'2026-01-01T12:00:00Z',
) -> None:
    body = _payload(event_id, sha256=sha256).replace(
        b'2026-01-01T12:00:00Z', occurred_at
    )
    await exchange.publish(
        Message(
            body,
            content_type='application/json',
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=event_id,
        ),
        routing_key=settings.routing_key,
        mandatory=True,
    )


async def _wait_for_source_event(
    repository: ProcessingJobRepository, event_id: str
) -> bool:
    return (
        await asyncio.to_thread(repository.get_by_source_event_id, event_id)
        is not None
    )


async def _assert_committed_event(
    repository: ProcessingJobRepository,
    event_id: str,
) -> tuple[ProcessingJob, ProcessingInput]:
    assert await _wait_until(
        lambda: _wait_for_source_event(repository, event_id)
    )
    accepted = repository.get_acceptance_by_source_event_id(event_id)
    assert accepted is not None
    job, processing_input = accepted
    assert job.status.value == 'RECEIVED'
    assert job.attempts == 0
    assert processing_input is not None
    return job, processing_input


async def _assert_quarantined_conflict(
    channel: aio_pika.abc.AbstractChannel,
    harness: ConsumerHarness,
    settings: WorkerSettings,
    event_id: str,
    metrics: ConsumerMetrics,
) -> None:
    await channel.queue_delete(settings.dead_letter_queue)
    await _publish_event(harness.exchange, settings, event_id, sha256='b' * 64)
    await asyncio.wait_for(harness.quarantine_prepared.wait(), timeout=5)
    dead_letter_queue = await channel.declare_queue(
        settings.dead_letter_queue, durable=True, passive=True
    )
    quarantined = None
    for _ in range(100):
        quarantined = await dead_letter_queue.get(fail=False, no_ack=False)
        if quarantined is not None:
            break
        await asyncio.sleep(0.05)
    assert quarantined is not None
    assert quarantined.body == _payload(event_id, sha256='b' * 64)
    await quarantined.ack()
    assert metrics.rejections.labels('conflict')._value.get() == 1


@pytest.mark.persistence
def test_real_delivery_duplicate_conflict_and_durable_input(
    database: DatabaseFixture,
) -> None:
    url = _amqp_url()
    engine = create_database_engine(database.url)
    repository = ProcessingJobRepository(create_session_factory(engine))
    token = uuid4().hex
    stop_event = asyncio.Event()
    registry = CollectorRegistry()
    metrics = ConsumerMetrics(registry)
    settings = _settings(token)

    async def scenario() -> None:
        admin_connection = await aio_pika.connect(url, timeout=5)
        consumer_connection = await aio_pika.connect(url, timeout=5)
        publisher_connection = await aio_pika.connect(url, timeout=5)
        channel = await admin_connection.channel(publisher_confirms=False)
        consumer_channel = await consumer_connection.channel(
            publisher_confirms=False
        )
        publisher_channel = await publisher_connection.channel()
        try:
            harness = await _start_test_consumer(
                ConsumerSetup(
                    channel,
                    consumer_channel,
                    publisher_channel,
                    repository,
                    metrics,
                    settings,
                    stop_event,
                    token,
                )
            )
            event_id = f'evt-{token}'
            await _publish_event(harness.exchange, settings, event_id)
            original_job, _original_input = await _assert_committed_event(
                repository, event_id
            )

            await _publish_event(
                harness.exchange,
                settings,
                event_id,
                occurred_at=b'2026-02-01T00:00:00Z',
            )
            assert await _wait_until(
                lambda: asyncio.sleep(
                    0,
                    result=metrics.acceptances.labels('duplicate')._value.get()
                    >= 1,
                )
            )
            assert repository.get_by_source_event_id(event_id) == original_job

            await _assert_quarantined_conflict(
                channel, harness, settings, event_id, metrics
            )

            stop_event.set()
            await harness.queue.cancel(harness.consumer_tag)
            await harness.consumer.wait_until_drained()
        finally:
            for connection in (
                publisher_connection,
                consumer_connection,
                admin_connection,
            ):
                if not connection.is_closed:
                    await connection.close()
        # Test resources are unique and owned by this test.
        cleanup = await aio_pika.connect(url, timeout=5)
        cleanup_channel = await cleanup.channel(publisher_confirms=False)
        for name in (
            settings.queue,
            settings.dead_letter_queue,
        ):
            await cleanup_channel.queue_delete(name)
        for name in (settings.exchange, settings.dead_letter_exchange):
            await cleanup_channel.exchange_delete(name)
        await cleanup.close()

    try:
        asyncio.run(scenario())
    finally:
        engine.dispose()


@pytest.mark.persistence
def test_committed_event_with_lost_ack_is_redelivered_as_duplicate(  # noqa: PLR0915
    database: DatabaseFixture,
) -> None:
    """Close the AMQP channel after commit and before EventConsumer can ACK."""
    url = _amqp_url()
    engine = create_database_engine(database.url)
    repository = ProcessingJobRepository(create_session_factory(engine))
    token = uuid4().hex
    event_id = f'ack-lost-{token}'
    stop_event = asyncio.Event()
    registry = CollectorRegistry()
    metrics = ConsumerMetrics(registry)
    settings = _settings(token)
    pause = CommitThenPauseAcceptance(repository)
    first_harness: ConsumerHarness | None = None
    second_harness: ConsumerHarness | None = None

    async def scenario() -> None:  # noqa: PLR0914
        admin_connection = await aio_pika.connect(url, timeout=5)
        consumer_connection = await aio_pika.connect(url, timeout=5)
        publisher_connection = await aio_pika.connect(url, timeout=5)
        admin_channel = await admin_connection.channel(
            publisher_confirms=False
        )
        first_channel = await consumer_connection.channel(
            publisher_confirms=False
        )
        publisher_channel = await publisher_connection.channel()
        try:
            nonlocal first_harness, second_harness
            first_harness = await _start_test_consumer(
                ConsumerSetup(
                    admin_channel,
                    first_channel,
                    publisher_channel,
                    repository,
                    metrics,
                    settings,
                    stop_event,
                    token,
                    pause,
                )
            )
            await _publish_event(first_harness.exchange, settings, event_id)
            assert await asyncio.to_thread(pause.committed.wait, 10)
            original = repository.get_acceptance_by_source_event_id(event_id)
            assert original is not None
            original_job, original_input = original
            assert original_input is not None
            assert original_job.status.value == 'RECEIVED'
            assert original_job.attempts == 0

            # Closing the channel makes this delivery unacknowledged at the
            # broker. The committed PostgreSQL row remains the idempotency key.
            await first_channel.close()
            pause.resume.set()
            await first_harness.consumer.wait_until_drained()

            second_channel = await consumer_connection.channel(
                publisher_confirms=False
            )
            second_harness = await _start_test_consumer(
                ConsumerSetup(
                    admin_channel,
                    second_channel,
                    publisher_channel,
                    repository,
                    metrics,
                    settings,
                    stop_event,
                    f'{token}-redelivery',
                )
            )
            assert await _wait_until(
                lambda: asyncio.sleep(
                    0,
                    result=(
                        metrics.acceptances.labels('duplicate')._value.get()
                        >= 1
                    ),
                )
            )
            assert await _wait_until(
                lambda: asyncio.sleep(
                    0,
                    result=metrics.ack_sent._value.get() >= 1,
                )
            )
            recovered = repository.get_acceptance_by_source_event_id(event_id)
            assert recovered == original
            assert repository.get_by_source_event_id(event_id) == original_job
            queue = await admin_channel.declare_queue(
                settings.queue, durable=True, passive=True
            )
            assert queue.declaration_result.message_count == 0
        finally:
            pause.resume.set()
            stop_event.set()
            for harness in (first_harness, second_harness):
                if (
                    harness is not None
                    and not harness.consumer_channel.is_closed
                ):
                    try:
                        await harness.queue.cancel(harness.consumer_tag)
                    except aio_pika.exceptions.ChannelInvalidStateError:
                        pass
                    await harness.consumer.wait_until_drained()
            for connection in (
                publisher_connection,
                consumer_connection,
                admin_connection,
            ):
                if not connection.is_closed:
                    await connection.close()
        cleanup = await aio_pika.connect(url, timeout=5)
        cleanup_channel = await cleanup.channel(publisher_confirms=False)
        for name in (settings.queue, settings.dead_letter_queue):
            await cleanup_channel.queue_delete(name)
        for name in (settings.exchange, settings.dead_letter_exchange):
            await cleanup_channel.exchange_delete(name)
        await cleanup.close()

    try:
        asyncio.run(scenario())
    finally:
        pause.resume.set()
        engine.dispose()
