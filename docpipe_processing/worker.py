"""Standalone RabbitMQ worker process for durable event acceptance."""

import asyncio
import logging
import os
import random
import signal
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import aio_pika
from aio_pika import ExchangeType
from aio_pika.exceptions import AMQPConnectionError
from prometheus_client import CollectorRegistry, start_http_server

from docpipe_processing.acceptance import EventAcceptance
from docpipe_processing.consumer import (
    ConsumerMetrics,
    ConsumerOperations,
    EventConsumer,
)
from docpipe_processing.observability import (
    configure_logging,
    shutdown_tracing,
)
from docpipe_processing.persistence import (
    PersistenceConfigurationError,
    PersistenceError,
    PersistenceUnavailableError,
    ProcessingJobRepository,
    create_database_engine,
    create_session_factory,
)

logger = logging.getLogger('docpipe_processing.worker')
MAX_TCP_PORT = 65535
DISTINCT_INPUT_AND_DLQ = 2


class WorkerConfigurationError(ValueError):
    """Worker-specific configuration is missing or malformed."""


class WorkerFatalError(RuntimeError):
    """A topology or internal error makes continuing unsafe."""


@dataclass(frozen=True, slots=True)
class WorkerSettings:
    amqp_url: str
    exchange: str
    exchange_type: str
    routing_key: str
    queue: str
    dead_letter_exchange: str
    dead_letter_queue: str
    dead_letter_routing_key: str
    metrics_host: str
    metrics_port: int
    heartbeat_seconds: int

    @classmethod
    def from_environment(cls) -> 'WorkerSettings':
        amqp_url = os.getenv('PROCESSING_AMQP_URL', '').strip()
        if not amqp_url:
            raise WorkerConfigurationError(
                'PROCESSING_AMQP_URL must be configured for the worker.'
            )

        exchange_type = os.getenv(
            'PROCESSING_AMQP_EXCHANGE_TYPE', 'direct'
        ).strip()
        if exchange_type not in {'direct', 'fanout', 'topic', 'headers'}:
            raise WorkerConfigurationError(
                'PROCESSING_AMQP_EXCHANGE_TYPE is unsupported.'
            )

        try:
            metrics_port = int(
                os.getenv('PROCESSING_WORKER_METRICS_PORT', '9000')
            )
            heartbeat_seconds = int(
                os.getenv('PROCESSING_AMQP_HEARTBEAT_SECONDS', '30')
            )
        except ValueError:
            raise WorkerConfigurationError(
                'Worker numeric configuration is invalid.'
            ) from None
        if not 1 <= metrics_port <= MAX_TCP_PORT or heartbeat_seconds <= 0:
            raise WorkerConfigurationError(
                'Worker numeric configuration is outside its valid range.'
            )

        settings = cls(
            amqp_url=amqp_url,
            exchange=os.getenv(
                'PROCESSING_AMQP_EXCHANGE', 'docpipe.events'
            ).strip(),
            exchange_type=exchange_type,
            routing_key=os.getenv(
                'PROCESSING_AMQP_ROUTING_KEY', 'document.received.v1'
            ).strip(),
            queue=os.getenv(
                'PROCESSING_AMQP_QUEUE',
                'processing.document-received.v1',
            ).strip(),
            dead_letter_exchange=os.getenv(
                'PROCESSING_AMQP_DLX', 'processing.dlx.v1'
            ).strip(),
            dead_letter_queue=os.getenv(
                'PROCESSING_AMQP_DLQ', 'processing.dead-letter.v1'
            ).strip(),
            dead_letter_routing_key=os.getenv(
                'PROCESSING_AMQP_DLQ_ROUTING_KEY', 'processing.rejected.v1'
            ).strip(),
            metrics_host=os.getenv(
                'PROCESSING_WORKER_METRICS_HOST', '127.0.0.1'
            ),
            metrics_port=metrics_port,
            heartbeat_seconds=heartbeat_seconds,
        )
        if any(
            not value
            for value in (
                settings.exchange,
                settings.routing_key,
                settings.queue,
                settings.dead_letter_exchange,
                settings.dead_letter_queue,
                settings.dead_letter_routing_key,
                settings.metrics_host,
            )
        ):
            raise WorkerConfigurationError(
                'Worker resource names and metrics host must be non-empty.'
            )
        if len({settings.queue, settings.dead_letter_queue}) != (
            DISTINCT_INPUT_AND_DLQ
        ):
            raise WorkerConfigurationError(
                'The input queue and dead-letter queue must be distinct.'
            )
        return settings


def input_queue_arguments(settings: WorkerSettings) -> dict[str, Any]:
    return {
        'x-queue-type': 'quorum',
        'x-dead-letter-exchange': settings.dead_letter_exchange,
        'x-dead-letter-routing-key': settings.dead_letter_routing_key,
        'x-dead-letter-strategy': 'at-least-once',
        'x-overflow': 'reject-publish',
        'x-delivery-limit': -1,
    }


async def declare_processing_topology(
    channel: aio_pika.abc.AbstractChannel,
    settings: WorkerSettings,
) -> aio_pika.abc.AbstractQueue:
    """Check the producer exchange; declare only Processing queues."""
    exchange = await channel.declare_exchange(
        settings.exchange,
        type=ExchangeType(settings.exchange_type),
        durable=True,
        passive=True,
    )
    dead_letter_exchange = await channel.declare_exchange(
        settings.dead_letter_exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    dead_letter_queue = await channel.declare_queue(
        settings.dead_letter_queue,
        durable=True,
        arguments={'x-queue-type': 'quorum'},
    )
    await dead_letter_queue.bind(
        dead_letter_exchange,
        routing_key=settings.dead_letter_routing_key,
    )
    queue = await channel.declare_queue(
        settings.queue,
        durable=True,
        arguments=input_queue_arguments(settings),
    )
    await queue.bind(exchange, routing_key=settings.routing_key)
    return queue


async def _database_probe(repository: ProcessingJobRepository) -> bool:
    try:
        await asyncio.to_thread(repository.get_by_processing_id, uuid4())
    except PersistenceUnavailableError:
        return False
    return True


async def _wait_for_database(
    repository: ProcessingJobRepository,
    stop_event: asyncio.Event,
) -> bool:
    delay = 1.0
    while not stop_event.is_set():
        if await _database_probe(repository):
            return True
        logger.warning(
            'worker_postgresql_unavailable',
            extra={
                'structured_event': {
                    'operation': 'worker.startup_probe',
                    'category': 'postgresql',
                    'state': 'suspended',
                }
            },
        )
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=random.uniform(delay * 0.8, delay * 1.2),
            )
        except TimeoutError:
            delay = min(delay * 2, 30.0)
        else:
            return False
    return False


async def _serve_connection(
    settings: WorkerSettings,
    repository: ProcessingJobRepository,
    acceptance: EventAcceptance,
    metrics: ConsumerMetrics,
    stop_event: asyncio.Event,
) -> None:
    connection = await aio_pika.connect(
        settings.amqp_url,
        timeout=5,
        heartbeat=settings.heartbeat_seconds,
        client_properties={'connection_name': 'docpipe-processing-worker'},
    )
    channel: aio_pika.abc.AbstractChannel | None = None
    consumer: EventConsumer | None = None
    consumer_tag: str | None = None
    generation_stop = asyncio.Event()
    wait_tasks: set[asyncio.Future[Any]] = set()
    queue: aio_pika.abc.AbstractQueue | None = None
    try:
        channel = await connection.channel(publisher_confirms=False)
        await channel.set_qos(prefetch_count=1)
        queue = await declare_processing_topology(channel, settings)

        async def probe_database() -> bool:
            return await _database_probe(repository)

        async def prepare_quarantine() -> None:
            await declare_processing_topology(channel, settings)

        consumer = EventConsumer(
            acceptance,
            ConsumerOperations(probe_database, prepare_quarantine),
            metrics,
            stop_event=generation_stop,
        )
        consumer_tag = await queue.consume(
            consumer.handle,
            no_ack=False,
            consumer_tag='docpipe-processing-worker',
        )
        logger.info(
            'worker_consumer_started',
            extra={
                'structured_event': {
                    'operation': 'worker.consume',
                    'state': 'ready',
                }
            },
        )

        closed_task = asyncio.ensure_future(connection.closed())
        channel_closed_task = asyncio.ensure_future(channel.closed())
        stop_task = asyncio.create_task(stop_event.wait())
        fatal_task = asyncio.create_task(consumer.fatal_error.wait())
        wait_tasks = {
            closed_task,
            channel_closed_task,
            stop_task,
            fatal_task,
        }
        await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
        if consumer.fatal_error.is_set():
            raise WorkerFatalError(
                'The consumer encountered an internal failure.'
            )
    finally:
        generation_stop.set()
        if (
            consumer_tag is not None
            and queue is not None
            and channel is not None
            and not channel.is_closed
        ):
            try:
                await queue.cancel(consumer_tag)
            except Exception:
                pass
        if consumer is not None:
            await consumer.wait_until_drained()
        for task in wait_tasks:
            if not task.done():
                task.cancel()
        if wait_tasks:
            await asyncio.gather(*wait_tasks, return_exceptions=True)
        if channel is not None and not channel.is_closed:
            await channel.close()
        if not connection.is_closed:
            await connection.close()


async def run_worker(
    stop_event: asyncio.Event | None = None,
) -> None:
    settings = WorkerSettings.from_environment()
    stop = stop_event or asyncio.Event()
    engine = create_database_engine()
    repository = ProcessingJobRepository(create_session_factory(engine))
    acceptance = EventAcceptance(repository)
    registry = CollectorRegistry()
    metrics = ConsumerMetrics(registry)
    metrics_server, _metrics_thread = start_http_server(
        port=settings.metrics_port,
        addr=settings.metrics_host,
        registry=registry,
    )
    reconnect_delay = 1.0
    try:
        while not stop.is_set():
            if not await _wait_for_database(repository, stop):
                break
            try:
                await _serve_connection(
                    settings,
                    repository,
                    acceptance,
                    metrics,
                    stop,
                )
                reconnect_delay = 1.0
            except asyncio.CancelledError:
                raise
            except WorkerFatalError:
                raise
            except AMQPConnectionError, TimeoutError, OSError:
                logger.warning(
                    'worker_connection_lost',
                    extra={
                        'structured_event': {
                            'operation': 'worker.connection',
                            'category': 'transport',
                            'state': 'recovering',
                        }
                    },
                )
                await asyncio.sleep(
                    random.uniform(
                        reconnect_delay * 0.8, reconnect_delay * 1.2
                    )
                )
                reconnect_delay = min(reconnect_delay * 2, 30.0)
            except Exception:
                raise WorkerFatalError(
                    'Worker topology validation failed.'
                ) from None
    finally:
        metrics_server.shutdown()
        metrics_server.server_close()
        engine.dispose()


def main() -> None:
    configure_logging()
    stop_event = asyncio.Event()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for signum in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(signum, stop_event.set)
    try:
        loop.run_until_complete(run_worker(stop_event))
    except (
        WorkerConfigurationError,
        PersistenceConfigurationError,
        PersistenceError,
    ) as error:
        logger.error(
            'worker_configuration_invalid',
            extra={
                'structured_event': {
                    'operation': 'worker.startup',
                    'category': 'configuration',
                    'state': 'stopped',
                }
            },
        )
        raise SystemExit(2) from error
    except WorkerFatalError:
        logger.error(
            'worker_stopped_after_internal_failure',
            extra={
                'structured_event': {
                    'operation': 'worker.shutdown',
                    'category': 'internal',
                    'state': 'stopped',
                }
            },
        )
        raise SystemExit(1) from None
    finally:
        shutdown_tracing()
        loop.close()


if __name__ == '__main__':
    main()
