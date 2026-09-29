import json
import logging
import os
import sys
from typing import Any, override

from opentelemetry import propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import (
    TraceContextTextMapPropagator,
)

SERVICE_NAME = 'docpipe-processing'


def configure_tracing() -> TracerProvider:
    provider = TracerProvider(
        resource=Resource.create({'service.name': SERVICE_NAME})
    )
    endpoint = os.getenv('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT')
    if endpoint:
        provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
        )

    trace.set_tracer_provider(provider)
    propagate.set_global_textmap(TraceContextTextMapPropagator())
    return provider


class JsonFormatter(logging.Formatter):
    @override
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            'service': SERVICE_NAME,
            'severity': record.levelname.lower(),
        }

        event = getattr(record, 'structured_event', None)
        if isinstance(event, dict):
            payload.update(event)
        else:
            payload['message'] = record.getMessage()

        return json.dumps(payload, ensure_ascii=False, separators=(',', ':'))


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)


TRACER_PROVIDER = configure_tracing()
TRACER = trace.get_tracer(SERVICE_NAME)


def shutdown_tracing() -> None:
    TRACER_PROVIDER.shutdown()
