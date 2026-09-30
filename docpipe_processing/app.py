import logging
import time
from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.responses import Response
from opentelemetry import propagate
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Histogram,
    generate_latest,
)
from starlette.middleware.base import RequestResponseEndpoint

from docpipe_processing.observability import SERVICE_NAME, TRACER

app = FastAPI(title='DocPipe Processing')
logger = logging.getLogger('docpipe_processing.http')

HTTP_REQUESTS = Counter(
    'docpipe_http_requests',
    'HTTP requests served by the Processing API.',
    ('method', 'route', 'status_code'),
)
HTTP_REQUEST_DURATION = Histogram(
    'docpipe_http_request_duration_seconds',
    'HTTP request duration in seconds.',
    ('method', 'route'),
)

HTTP_METHODS = {'DELETE', 'GET', 'HEAD', 'OPTIONS', 'PATCH', 'POST', 'PUT'}


@app.middleware('http')
async def observe_http_request(
    request: Request, call_next: RequestResponseEndpoint
) -> Response:
    started_at = time.perf_counter()
    status_code = 500
    parent_context = propagate.extract(request.headers)

    with TRACER.start_as_current_span(
        'http.request',
        context=parent_context,
        kind=SpanKind.SERVER,
    ) as span:
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            span.set_status(Status(StatusCode.ERROR))
            raise
        finally:
            duration = time.perf_counter() - started_at
            method = (
                request.method if request.method in HTTP_METHODS else 'OTHER'
            )
            route = getattr(request.scope.get('route'), 'path', 'unmatched')
            status = str(status_code)
            failed = status_code >= HTTPStatus.INTERNAL_SERVER_ERROR

            span.update_name(f'{method} {route}')
            span.set_attribute('http.request.method', method)
            span.set_attribute('http.route', route)
            span.set_attribute('http.response.status_code', status_code)
            if failed:
                span.set_status(Status(StatusCode.ERROR))

            HTTP_REQUESTS.labels(method, route, status).inc()
            HTTP_REQUEST_DURATION.labels(method, route).observe(duration)

            trace_id = span.get_span_context().trace_id
            logger.info(
                'http_request',
                extra={
                    'structured_event': {
                        'service': SERVICE_NAME,
                        'operation': 'http.request',
                        'method': method,
                        'route': route,
                        'status_code': status_code,
                        'state': 'failed' if failed else 'completed',
                        'duration_seconds': duration,
                        'trace_id': f'{trace_id:032x}',
                    }
                },
            )


@app.get('/metrics', include_in_schema=False)
def metrics() -> Response:
    return Response(
        content=generate_latest(),
        headers={'Content-Type': CONTENT_TYPE_LATEST},
    )


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}
