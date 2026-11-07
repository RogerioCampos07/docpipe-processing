"""RabbitMQ delivery handling for durable Processing acceptance."""

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

import aio_pika
from opentelemetry import propagate
from opentelemetry.trace import Span, SpanKind, Status, StatusCode
from prometheus_client import CollectorRegistry, Counter, Histogram

from docpipe_processing.acceptance import (
    AcceptanceUnavailableError,
    AcceptedEvent,
    ConflictingEventError,
    EventAcceptance,
    IncompleteLegacyEventError,
    PermanentEventError,
)
from docpipe_processing.observability import TRACER

logger = logging.getLogger('docpipe_processing.consumer')


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded local retry and capped PostgreSQL recovery intervals."""

    transient_delays: tuple[float, ...] = (1.0, 5.0)
    recovery_delays: tuple[float, ...] = (1, 2, 5, 10, 30)


@dataclass(frozen=True, slots=True)
class ConsumerOperations:
    """Infrastructure probes and topology checks required by delivery."""

    database_probe: Callable[[], Awaitable[bool]]
    prepare_quarantine: Callable[[], Awaitable[None]]


class ConsumerMetrics:
    """Low-cardinality metrics owned by the worker process."""

    def __init__(self, registry: CollectorRegistry) -> None:
        self.deliveries = Counter(
            'docpipe_worker_deliveries_total',
            'AMQP deliveries received by the Processing worker.',
            registry=registry,
        )
        self.acceptances = Counter(
            'docpipe_worker_acceptances_total',
            'Durable Processing event outcomes.',
            ('result',),
            registry=registry,
        )
        self.failures = Counter(
            'docpipe_worker_failures_total',
            'Acceptance failures by safe category.',
            ('category',),
            registry=registry,
        )
        self.suspensions = Counter(
            'docpipe_worker_suspensions_total',
            'Consumer suspensions while PostgreSQL recovers.',
            registry=registry,
        )
        self.recoveries = Counter(
            'docpipe_worker_recoveries_total',
            'Consumer recoveries after PostgreSQL probes succeed.',
            registry=registry,
        )
        self.ack_attempts = Counter(
            'docpipe_worker_ack_attempts_total',
            'Manual ACK operations attempted.',
            registry=registry,
        )
        self.ack_sent = Counter(
            'docpipe_worker_ack_sent_total',
            'Manual ACK operations written to the AMQP channel.',
            registry=registry,
        )
        self.rejections = Counter(
            'docpipe_worker_rejections_total',
            'Permanent messages rejected without requeue.',
            ('category',),
            registry=registry,
        )
        self.acceptance_duration = Histogram(
            'docpipe_worker_acceptance_duration_seconds',
            'Time spent establishing durable event acceptance.',
            registry=registry,
        )


class EventConsumer:
    """Apply the acceptance outcome to AMQP manual acknowledgements."""

    def __init__(
        self,
        acceptance: EventAcceptance,
        operations: ConsumerOperations,
        metrics: ConsumerMetrics,
        *,
        stop_event: asyncio.Event,
        retry_policy: RetryPolicy = RetryPolicy(),
    ) -> None:
        self._acceptance = acceptance
        self._operations = operations
        self._metrics = metrics
        self._stop_event = stop_event
        self._retry_policy = retry_policy
        self.fatal_error = asyncio.Event()
        self._active_callbacks = 0
        self._drained = asyncio.Event()
        self._drained.set()

    async def handle(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        self._active_callbacks += 1
        self._drained.clear()
        self._metrics.deliveries.inc()
        started = time.perf_counter()
        headers = message.headers or {}
        trace_headers = {
            name: value
            for name in ('traceparent', 'tracestate')
            if isinstance((value := headers.get(name)), str)
        }
        parent_context = propagate.extract(trace_headers)
        try:
            with TRACER.start_as_current_span(
                'processing.accept_event',
                context=parent_context,
                kind=SpanKind.CONSUMER,
            ) as span:
                await self._accept_and_ack(message, span)
        except asyncio.CancelledError:
            raise
        except Exception:
            self._metrics.failures.labels('internal').inc()
            self.fatal_error.set()
            logger.error(
                'consumer_delivery_suspended',
                extra={
                    'structured_event': {
                        'operation': 'processing.accept_event',
                        'category': 'internal',
                        'state': 'suspended',
                    }
                },
            )
        finally:
            self._metrics.acceptance_duration.observe(
                time.perf_counter() - started
            )
            self._active_callbacks -= 1
            if self._active_callbacks == 0:
                self._drained.set()

    async def wait_until_drained(self) -> None:
        """Wait for database work to finish before disposing its engine."""
        await self._drained.wait()

    async def _accept_and_ack(
        self,
        message: aio_pika.abc.AbstractIncomingMessage,
        span: Span,
    ) -> None:
        attempt = 0
        recovering = False
        while not self._stop_event.is_set():
            attempt += 1
            try:
                outcome = await self._run_acceptance(message.body)
            except PermanentEventError as error:
                if isinstance(error, ConflictingEventError):
                    category = 'conflict'
                elif isinstance(error, IncompleteLegacyEventError):
                    category = 'legacy'
                else:
                    category = 'invalid'
                self._metrics.failures.labels(category).inc()
                self._metrics.rejections.labels(category).inc()
                await self._operations.prepare_quarantine()
                await message.reject(requeue=False)
                logger.info(
                    'consumer_event_rejected',
                    extra={
                        'structured_event': {
                            'operation': 'processing.accept_event',
                            'category': category,
                            'state': 'rejected',
                        }
                    },
                )
                return
            except AcceptanceUnavailableError:
                self._metrics.failures.labels('postgresql').inc()
                if recovering:
                    if not await self._wait_for_database():
                        return
                    self._metrics.recoveries.inc()
                    recovering = False
                    attempt = 0
                    continue
                if attempt <= len(self._retry_policy.transient_delays):
                    await self._sleep_or_stop(
                        self._retry_policy.transient_delays[attempt - 1]
                    )
                    continue
                self._metrics.suspensions.inc()
                logger.warning(
                    'consumer_postgresql_suspended',
                    extra={
                        'structured_event': {
                            'operation': 'processing.accept_event',
                            'category': 'postgresql',
                            'state': 'suspended',
                        }
                    },
                )
                recovering = True
                if not await self._wait_for_database():
                    return
                self._metrics.recoveries.inc()
                recovering = False
                attempt = 0
                continue
            except Exception:
                span.set_status(Status(StatusCode.ERROR))
                # Keep the error category, not its text or chained cause.
                raise RuntimeError('Acceptance failed internally.') from None

            span.set_attribute(
                'processing.acceptance.result', outcome.result.value
            )
            self._metrics.acceptances.labels(outcome.result.value).inc()
            if message.channel.is_closed:
                return
            self._metrics.ack_attempts.inc()
            await message.ack(multiple=False)
            self._metrics.ack_sent.inc()
            logger.info(
                'consumer_event_accepted',
                extra={
                    'structured_event': {
                        'operation': 'processing.accept_event',
                        'result': outcome.result.value,
                        'state': 'acknowledged',
                    }
                },
            )
            return

    async def _run_acceptance(self, body: bytes) -> AcceptedEvent:
        task = asyncio.create_task(
            asyncio.to_thread(self._acceptance.accept, body)
        )
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    async def _wait_for_database(self) -> bool:
        attempt = 0
        while not self._stop_event.is_set():
            if await self._operations.database_probe():
                return True
            self._metrics.failures.labels('postgresql_probe').inc()
            delay = self._retry_policy.recovery_delays[
                min(attempt, len(self._retry_policy.recovery_delays) - 1)
            ]
            attempt += 1
            if not await self._sleep_or_stop(
                random.uniform(delay * 0.8, delay * 1.2)
            ):
                return False
        return False

    async def _sleep_or_stop(self, seconds: float) -> bool:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except TimeoutError:
            return not self._stop_event.is_set()
        return False
