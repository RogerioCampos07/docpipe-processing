"""Versioned, read-only HTTP projection of Processing jobs."""

import logging
from collections.abc import Callable, Coroutine
from http import HTTPStatus
from typing import Annotated, Any, Literal, cast, override
from uuid import UUID

from fastapi import APIRouter, Depends, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from docpipe_processing.application import (
    ProcessingJobReader,
    get_processing_job,
)
from docpipe_processing.domain import ProcessingJob, ProcessingStatus
from docpipe_processing.observability import SERVICE_NAME
from docpipe_processing.persistence import (
    PersistenceConfigurationError,
    PersistenceUnavailableError,
)

logger = logging.getLogger('docpipe_processing.api')
ErrorCode = Literal[
    'processing_job_not_found',
    'invalid_processing_id',
    'persistence_unavailable',
    'internal_error',
]


class ProcessingJobResponse(BaseModel):
    """Public snapshot; it does not guarantee document recoverability."""

    model_config = ConfigDict(strict=True, extra='forbid', frozen=True)

    processing_id: UUID
    source_event_id: str
    document_id: str
    correlation_id: str | None
    status: ProcessingStatus
    attempts: int = Field(ge=0)
    created_at: AwareDatetime
    updated_at: AwareDatetime
    started_at: AwareDatetime | None
    finished_at: AwareDatetime | None
    failure_reason: str | None = Field(
        description='Generic public message for FAILED; otherwise null.'
    )

    @classmethod
    def from_job(cls, job: ProcessingJob) -> 'ProcessingJobResponse':
        return cls(
            processing_id=job.processing_id,
            source_event_id=job.source_event_id,
            document_id=job.document_id,
            correlation_id=job.correlation_id,
            status=job.status,
            attempts=job.attempts,
            created_at=job.created_at,
            updated_at=job.updated_at,
            started_at=job.started_at,
            finished_at=job.finished_at,
            failure_reason=(
                'O processamento falhou.'
                if job.status is ProcessingStatus.FAILED
                else None
            ),
        )


class ErrorResponse(BaseModel):
    """Safe error without exception details or echoed request values."""

    model_config = ConfigDict(extra='forbid')

    code: ErrorCode
    message: str


def error_response(
    status_code: int, code: ErrorCode, message: str
) -> JSONResponse:
    if status_code >= HTTPStatus.INTERNAL_SERVER_ERROR:
        logger.error(
            'processing_job_query_failed',
            extra={
                'structured_event': {
                    'service': SERVICE_NAME,
                    'operation': 'processing_job.query',
                    'code': code,
                    'status_code': status_code,
                }
            },
        )
    return JSONResponse(
        status_code=status_code,
        content=ErrorResponse(code=code, message=message).model_dump(),
        headers={'Cache-Control': 'no-store'},
    )


class ProcessingJobRoute(APIRoute):
    """Contain validation and execution failures before HTTP tracing.

    Scope the error contract to this router, including dependency resolution
    and response validation. Never pass raw exceptions to logs or traces.
    """

    @override
    def get_route_handler(
        self,
    ) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def safe_handler(request: Request) -> Response:
            try:
                return await handler(request)
            except RequestValidationError:
                return error_response(
                    422, 'invalid_processing_id', 'Identificador inválido.'
                )
            except PersistenceConfigurationError, PersistenceUnavailableError:
                return error_response(
                    503,
                    'persistence_unavailable',
                    'Consulta temporariamente indisponível.',
                )
            except Exception:
                return error_response(
                    500,
                    'internal_error',
                    'Não foi possível concluir a consulta.',
                )

        return safe_handler


router = APIRouter(route_class=ProcessingJobRoute)


def get_job_reader(request: Request) -> ProcessingJobReader | None:
    return cast(ProcessingJobReader | None, request.app.state.job_reader)


@router.get(
    '/v1/processing-jobs/{processing_id}',
    response_model=ProcessingJobResponse,
    summary='Consult a persisted Processing job',
    description=(
        'Read the current job snapshot from Processing-owned PostgreSQL. '
        'Does not start processing or guarantee document recoverability.'
    ),
    responses={
        404: {'model': ErrorResponse, 'description': 'Job not found'},
        422: {'model': ErrorResponse, 'description': 'Invalid UUID'},
        503: {
            'model': ErrorResponse,
            'description': 'Persistence unavailable',
        },
        500: {'model': ErrorResponse, 'description': 'Internal failure'},
    },
)
def read_processing_job(
    processing_id: UUID,
    response: Response,
    reader: Annotated[ProcessingJobReader | None, Depends(get_job_reader)],
) -> ProcessingJobResponse | JSONResponse:
    if reader is None:
        raise PersistenceConfigurationError('Persistence is not configured.')
    job = get_processing_job(processing_id, reader)
    if job is None:
        return error_response(
            404, 'processing_job_not_found', 'Processamento não encontrado.'
        )
    response.headers['Cache-Control'] = 'no-store'
    return ProcessingJobResponse.from_job(job)
