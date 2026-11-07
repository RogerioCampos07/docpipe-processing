"""RabbitMQ 4.3.6 quorum delivery and dead-letter recovery proofs."""

import asyncio
import os
import subprocess
import time
from dataclasses import dataclass, replace
from typing import Any
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import aio_pika
import pytest
from aio_pika import DeliveryMode, ExchangeType, Message

from docpipe_processing.worker import (
    WorkerSettings,
    input_queue_arguments,
)

pytestmark = [pytest.mark.integration, pytest.mark.consumer]
REDELIVERY_CHECK_COUNT = 26


@dataclass
class RecoveryTopology:
    connection: aio_pika.abc.AbstractConnection
    channel: aio_pika.abc.AbstractChannel
    source_exchange: aio_pika.abc.AbstractExchange
    dead_letter_exchange: aio_pika.abc.AbstractExchange
    source_queue: aio_pika.abc.AbstractQueue
    dead_letter_queue: aio_pika.abc.AbstractQueue


def _url() -> str:
    value = os.getenv('PROCESSING_AMQP_URL', '')
    if not value:
        pytest.skip('Set PROCESSING_AMQP_URL to an isolated RabbitMQ vhost.')
    container_id = os.getenv('PROCESSING_RABBITMQ_CONTAINER_ID', '').strip()
    if container_id:
        value = _url_after_container_restart(value, container_id)
    return value


def _url_after_container_restart(url: str, container_id: str) -> str:
    """Refresh a dynamically published host port after Docker restarts."""
    result = subprocess.run(
        ['docker', 'port', container_id, '5672/tcp'],
        capture_output=True,
        check=True,
        text=True,
        timeout=10,
    )
    host_port = result.stdout.strip().rsplit(':', maxsplit=1)[-1]
    parsed = urlsplit(url)
    if parsed.hostname is None:
        raise AssertionError('RabbitMQ test URL must include a hostname.')
    credentials = ''
    if parsed.username is not None:
        credentials = parsed.username
        if parsed.password is not None:
            credentials += f':{parsed.password}'
        credentials += '@'
    host = (
        f'[{parsed.hostname}]' if ':' in parsed.hostname else parsed.hostname
    )
    return urlunsplit((
        parsed.scheme,
        f'{credentials}{host}:{host_port}',
        parsed.path,
        parsed.query,
        parsed.fragment,
    ))


def _settings(token: str) -> WorkerSettings:
    return WorkerSettings(
        amqp_url=_url(),
        exchange=f'recovery.events.{token}',
        exchange_type='direct',
        routing_key='document.received.v1',
        queue=f'processing.recovery.{token}',
        dead_letter_exchange=f'processing.recovery.dlx.{token}',
        dead_letter_queue=f'processing.recovery.dead.{token}',
        dead_letter_routing_key='processing.rejected.v1',
        metrics_host='127.0.0.1',
        metrics_port=9000,
        heartbeat_seconds=30,
    )


async def _declare_test_topology(
    settings: WorkerSettings, *, limit_dead_letter_queue: bool = False
) -> RecoveryTopology:
    connection = await aio_pika.connect(settings.amqp_url, timeout=5)
    channel = await connection.channel()
    source_exchange = await channel.declare_exchange(
        settings.exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    dead_letter_exchange = await channel.declare_exchange(
        settings.dead_letter_exchange,
        type=ExchangeType.DIRECT,
        durable=True,
    )
    dead_arguments: dict[str, Any] = {'x-queue-type': 'quorum'}
    if limit_dead_letter_queue:
        dead_arguments.update({
            'x-max-length': 1,
            'x-overflow': 'reject-publish',
        })
    dead_letter_queue = await channel.declare_queue(
        settings.dead_letter_queue,
        durable=True,
        arguments=dead_arguments,
    )
    await dead_letter_queue.bind(
        dead_letter_exchange,
        routing_key=settings.dead_letter_routing_key,
    )
    source_queue = await channel.declare_queue(
        settings.queue,
        durable=True,
        arguments=input_queue_arguments(settings),
    )
    await source_queue.bind(source_exchange, routing_key=settings.routing_key)
    return RecoveryTopology(
        connection,
        channel,
        source_exchange,
        dead_letter_exchange,
        source_queue,
        dead_letter_queue,
    )


async def _cleanup_test_topology(settings: WorkerSettings) -> None:
    connection = await aio_pika.connect(settings.amqp_url, timeout=5)
    channel = await connection.channel(publisher_confirms=False)
    await channel.queue_delete(settings.queue)
    await channel.queue_delete(settings.dead_letter_queue)
    await channel.exchange_delete(settings.exchange)
    await channel.exchange_delete(settings.dead_letter_exchange)
    await connection.close()


@pytest.mark.asyncio
async def test_delivery_limit_disabled_and_full_dlq_retains_message() -> None:
    token = uuid4().hex
    settings = _settings(token)
    topology = await _declare_test_topology(
        settings, limit_dead_letter_queue=True
    )
    try:
        redelivery_id = f'limit-{token}'
        await topology.source_exchange.publish(
            Message(
                redelivery_id.encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
            ),
            routing_key=settings.routing_key,
            mandatory=True,
        )
        for _ in range(REDELIVERY_CHECK_COUNT):
            redelivered = await topology.source_queue.get(
                timeout=3, fail=False
            )
            assert redelivered is not None
            assert redelivered.body == redelivery_id.encode()
            await redelivered.nack(requeue=True)
        remaining = await topology.source_queue.get(timeout=3, fail=False)
        assert remaining is not None
        assert remaining.body == redelivery_id.encode()
        await remaining.ack()
        assert (
            await topology.dead_letter_queue.get(timeout=1, fail=False) is None
        )

        await topology.dead_letter_exchange.publish(
            Message(b'capacity-marker', delivery_mode=DeliveryMode.PERSISTENT),
            routing_key=settings.dead_letter_routing_key,
            mandatory=True,
        )
        capacity_marker = await topology.dead_letter_queue.get(
            timeout=3, fail=False
        )
        assert capacity_marker is not None
        await capacity_marker.nack(requeue=True)

        rejected_id = f'rejected-{token}'
        await topology.source_exchange.publish(
            Message(
                rejected_id.encode(),
                delivery_mode=DeliveryMode.PERSISTENT,
                message_id=rejected_id,
            ),
            routing_key=settings.routing_key,
            mandatory=True,
        )
        rejected = await topology.source_queue.get(timeout=3, fail=False)
        assert rejected is not None
        assert rejected.body == rejected_id.encode()
        await rejected.reject(requeue=False)
        await asyncio.sleep(4)

        awaitable_marker = await topology.dead_letter_queue.get(
            timeout=3, fail=False
        )
        assert awaitable_marker is not None
        assert awaitable_marker.body == b'capacity-marker'
        await awaitable_marker.ack()

        recovered = None
        for _ in range(30):
            recovered = await topology.dead_letter_queue.get(
                timeout=1, fail=False
            )
            if recovered is not None:
                break
            await asyncio.sleep(0.2)
        assert recovered is not None
        assert recovered.body == rejected_id.encode()
        assert recovered.message_id == rejected_id
        await recovered.ack()
    finally:
        await topology.connection.close()
        await _cleanup_test_topology(settings)


@pytest.mark.asyncio
async def test_persistent_source_and_dlq_messages_survive_broker_restart() -> (
    None
):
    container_id = os.getenv('PROCESSING_RABBITMQ_CONTAINER_ID', '').strip()
    if not container_id:
        pytest.skip(
            'Set PROCESSING_RABBITMQ_CONTAINER_ID to a disposable test broker.'
        )
    settings = _settings(uuid4().hex)
    topology = await _declare_test_topology(settings)
    source_id = f'source-{uuid4()}'
    dead_id = f'dead-{uuid4()}'
    await topology.source_exchange.publish(
        Message(
            source_id.encode(),
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=source_id,
        ),
        routing_key=settings.routing_key,
        mandatory=True,
    )
    await topology.dead_letter_exchange.publish(
        Message(
            dead_id.encode(),
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=dead_id,
        ),
        routing_key=settings.dead_letter_routing_key,
        mandatory=True,
    )
    await topology.connection.close()

    result = subprocess.run(
        ['docker', 'restart', '--time', '20', container_id],
        capture_output=True,
        check=False,
        timeout=90,
    )
    assert result.returncode == 0
    settings = replace(
        settings,
        amqp_url=_url_after_container_restart(settings.amqp_url, container_id),
    )

    recovered_connection = None
    # Quorum queue recovery can make RabbitMQ 4.3.6 boot slower than a
    # non-quorum single-queue restart in a container with several test queues.
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            recovered_connection = await aio_pika.connect(
                settings.amqp_url,
                timeout=3,
            )
            break
        except OSError, TimeoutError, aio_pika.exceptions.AMQPConnectionError:
            await asyncio.sleep(1)
    assert recovered_connection is not None
    try:
        recovered_channel = await recovered_connection.channel()
        recovered_source = await recovered_channel.declare_queue(
            settings.queue, durable=True, passive=True
        )
        recovered_dead = await recovered_channel.declare_queue(
            settings.dead_letter_queue, durable=True, passive=True
        )
        source_message = await recovered_source.get(timeout=3, fail=False)
        dead_message = await recovered_dead.get(timeout=3, fail=False)
        assert source_message is not None
        assert dead_message is not None
        assert source_message.body == source_id.encode()
        assert dead_message.body == dead_id.encode()
        assert source_message.message_id == source_id
        assert dead_message.message_id == dead_id
        await source_message.ack()
        await dead_message.ack()
    finally:
        await recovered_connection.close()
        await _cleanup_test_topology(settings)
