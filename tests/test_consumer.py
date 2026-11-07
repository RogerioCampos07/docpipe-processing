"""Unit tests for manual ACK/NACK and pause behavior."""

import asyncio
import threading
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast
from uuid import UUID

import aio_pika
import pytest
from prometheus_client import CollectorRegistry

from docpipe_processing.acceptance import (
    AcceptanceResult,
    AcceptanceUnavailableError,
    AcceptedEvent,
    EventAcceptance,
    PermanentEventError,
)
from docpipe_processing.consumer import (
    ConsumerMetrics,
    ConsumerOperations,
    EventConsumer,
    RetryPolicy,
)
from docpipe_processing.domain import ProcessingJob
from tests.test_acceptance import FakeAcceptanceStore, event_payload

pytestmark = pytest.mark.unit
EXPECTED_ACCEPTANCE_CALLS_AFTER_RECOVERY = 4

PROCESSING_ID = UUID('12345678-1234-5678-1234-567812345678')
JOB = ProcessingJob.create(
    processing_id=PROCESSING_ID,
    source_event_id='synthetic-event',
    document_id='synthetic-document',
    created_at=datetime(2026, 1, 1, tzinfo=UTC),
)


class FakeMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.headers: dict[str, object] = {'traceparent': 'invalid-context'}
        self.channel: Any = SimpleNamespace(is_closed=False)
        self.ack_calls: list[bool] = []
        self.reject_calls: list[bool] = []

    async def ack(self, *, multiple: bool = False) -> None:
        self.ack_calls.append(multiple)

    async def reject(self, *, requeue: bool = False) -> None:
        self.reject_calls.append(requeue)


class ScriptedAcceptance:
    def __init__(self, results: list[AcceptedEvent | Exception]) -> None:
        self.results = list(results)
        self.calls = 0

    def accept(self, _body: bytes) -> AcceptedEvent:
        self.calls += 1
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def make_consumer(
    acceptance: object,
    *,
    probe: object = None,
    stop_event: asyncio.Event | None = None,
) -> EventConsumer:
    async def default_probe() -> bool:
        return True

    async def prepare_quarantine() -> None:
        return None

    return EventConsumer(
        cast(EventAcceptance, acceptance),
        ConsumerOperations(
            probe or default_probe,  # type: ignore[arg-type]
            prepare_quarantine,
        ),
        ConsumerMetrics(CollectorRegistry()),
        stop_event=stop_event or asyncio.Event(),
        retry_policy=RetryPolicy(
            transient_delays=(0, 0),
            recovery_delays=(0,),
        ),
    )


def test_ack_occurs_only_after_acceptance_returns_durable_outcome() -> None:
    committed = threading.Event()

    class DelayedStore(FakeAcceptanceStore):
        def create(self, job, *, input):  # type: ignore[no-untyped-def]
            committed.wait(timeout=3)
            return super().create(job, input=input)

    store = DelayedStore()
    acceptance = EventAcceptance(
        store,
        now=lambda: datetime(2026, 1, 1, tzinfo=UTC),
        new_id=lambda: PROCESSING_ID,
    )
    consumer = make_consumer(acceptance)
    message = FakeMessage(event_payload())

    async def scenario() -> None:
        task = asyncio.create_task(
            consumer.handle(
                cast(aio_pika.abc.AbstractIncomingMessage, message)
            )
        )
        await asyncio.sleep(0.05)
        assert message.ack_calls == []
        committed.set()
        await task

    asyncio.run(scenario())

    assert message.ack_calls == [False]
    assert store.rows['evt-synthetic-0001'][0].attempts == 0


def test_permanent_contract_failure_is_rejected_without_requeue() -> None:
    acceptance = ScriptedAcceptance([PermanentEventError('invalid')])
    consumer = make_consumer(acceptance)
    message = FakeMessage(b'invalid-json')

    asyncio.run(
        consumer.handle(cast(aio_pika.abc.AbstractIncomingMessage, message))
    )

    assert message.ack_calls == []
    assert message.reject_calls == [False]


def test_postgresql_failure_retries_then_recovers_without_requeue() -> None:
    outcome = AcceptedEvent(AcceptanceResult.ACCEPTED, JOB)
    acceptance = ScriptedAcceptance([
        AcceptanceUnavailableError('temporary'),
        AcceptanceUnavailableError('temporary'),
        AcceptanceUnavailableError('temporary'),
        outcome,
    ])
    probes = 0

    async def probe() -> bool:
        nonlocal probes
        probes += 1
        return True

    consumer = make_consumer(acceptance, probe=probe)
    message = FakeMessage(b'synthetic-event')

    asyncio.run(
        consumer.handle(cast(aio_pika.abc.AbstractIncomingMessage, message))
    )

    assert acceptance.calls == EXPECTED_ACCEPTANCE_CALLS_AFTER_RECOVERY
    assert probes == 1
    assert message.ack_calls == [False]
    assert message.reject_calls == []


def test_shutdown_during_database_pause_leaves_delivery_unacknowledged() -> (
    None
):
    stop_event = asyncio.Event()
    acceptance = ScriptedAcceptance([
        AcceptanceUnavailableError('temporary'),
        AcceptanceUnavailableError('temporary'),
        AcceptanceUnavailableError('temporary'),
    ])

    async def probe() -> bool:
        return False

    consumer = make_consumer(
        acceptance,
        probe=probe,
        stop_event=stop_event,
    )
    message = FakeMessage(b'synthetic-event')

    async def scenario() -> None:
        task = asyncio.create_task(
            consumer.handle(
                cast(aio_pika.abc.AbstractIncomingMessage, message)
            )
        )
        await asyncio.sleep(0.05)
        stop_event.set()
        await task

    asyncio.run(scenario())

    assert message.ack_calls == []
    assert message.reject_calls == []
