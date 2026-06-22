from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import Settings, get_settings
from app.database import check_database_ready, create_tables
from app.dependencies import get_job_producer, get_job_service
from app.enums import JobStatus
from app.errors import DependencyUnavailableError, ServiceError
from app.schemas import (
    CancelResponse,
    DrainRequest,
    DrainResponse,
    JobCreate,
    JobCreateResponse,
    JobListItem,
    JobListResponse,
    JobResponse,
    MetricsResponse,
    QueueDepthPriority,
    QueueDepthResponse,
)
from app.service import JobService, success_failure_rates


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        settings = get_settings()
        request_id = request.headers.get(settings.request_id_header, str(uuid4()))
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers[settings.request_id_header] = request_id
        return response


def error_response(request: Request, code: str, message: str, status_code: int, details: dict | None = None) -> JSONResponse:
    request_id = getattr(request.state, "request_id", str(uuid4()))
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "message": message, "request_id": request_id, "details": details or {}}},
    )


def create_app() -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        settings = get_settings()
        if settings.create_tables_on_startup:
            create_tables()
        yield

    app = FastAPI(title="Async Job Processing Service", lifespan=lifespan)
    app.add_middleware(RequestIdMiddleware)

    @app.exception_handler(ServiceError)
    async def handle_service_error(request: Request, exc: ServiceError) -> JSONResponse:
        return error_response(request, exc.code, exc.message, exc.status_code, exc.details)

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return error_response(request, "validation_error", "Request validation failed", 422, {"errors": exc.errors()})

    @app.exception_handler(Exception)
    async def handle_unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        return error_response(request, "internal_server_error", "Unexpected server error", 500)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> dict[str, str]:
        database_ok = check_database_ready()
        kafka_ok = get_job_producer().ready()
        if not database_ok or not kafka_ok:
            raise DependencyUnavailableError("Dependency unavailable", {"database": database_ok, "kafka": kafka_ok})
        return {"status": "ready", "database": "ok", "kafka": "ok"}

    @app.post("/jobs", response_model=JobCreateResponse, status_code=201)
    def create_job(
        request: JobCreate,
        service: Annotated[JobService, Depends(get_job_service)],
    ):
        job, replay = service.submit_job(request)
        status_code = 200 if replay else 201
        response = JSONResponse(
            status_code=status_code,
            content={"id": str(job.id), "status": job.status.value, "idempotent_replay": replay},
        )
        return response

    @app.get("/jobs/{job_id}", response_model=JobResponse)
    def get_job(job_id: UUID, service: Annotated[JobService, Depends(get_job_service)]) -> JobResponse:
        return JobResponse.model_validate(service.get_job(job_id))

    @app.get("/jobs", response_model=JobListResponse)
    def list_jobs(
        service: Annotated[JobService, Depends(get_job_service)],
        status: JobStatus | None = None,
        handler: str | None = None,
        limit: int = Query(default=50, ge=1),
        offset: int = Query(default=0, ge=0),
    ) -> JobListResponse:
        jobs = service.list_jobs(status, handler, limit, offset)
        return JobListResponse(
            items=[
                JobListItem(id=job.id, handler=job.handler, status=job.status, attempt_count=job.attempt_count, created_at=job.created_at)
                for job in jobs
            ],
            limit=min(limit, get_settings().max_page_size),
            offset=offset,
        )

    @app.get("/queue/depth", response_model=QueueDepthResponse)
    def queue_depth(service: Annotated[JobService, Depends(get_job_service)]) -> QueueDepthResponse:
        depth = service.queue_depth()
        return QueueDepthResponse(
            queued=depth.queued,
            due=depth.due,
            running=depth.running,
            dead_lettered=depth.dead_lettered,
            by_priority=[QueueDepthPriority(**item) for item in depth.by_priority],
        )

    @app.get("/metrics", response_model=MetricsResponse)
    def metrics(service: Annotated[JobService, Depends(get_job_service)]) -> MetricsResponse:
        metric_values = service.metrics()
        success_rate, failure_rate = success_failure_rates(metric_values)
        return MetricsResponse(
            job_success_count=metric_values.job_success_count,
            job_failure_count=metric_values.job_failure_count + metric_values.dead_letter_count,
            job_success_rate=success_rate,
            job_failure_rate=failure_rate,
            retry_count=metric_values.retry_count,
            dead_letter_count=metric_values.dead_letter_count,
            job_latency_p50_seconds=metric_values.job_latency_p50_seconds,
            job_latency_p95_seconds=metric_values.job_latency_p95_seconds,
            worker_utilization=metric_values.worker_utilization,
        )

    @app.post("/jobs/{job_id}/cancel", response_model=CancelResponse)
    def cancel_job(job_id: UUID, service: Annotated[JobService, Depends(get_job_service)]) -> CancelResponse:
        job = service.cancel_job(job_id)
        return CancelResponse(id=job.id, status=job.status)

    @app.post("/ops/drain", response_model=DrainResponse)
    def set_drain(request: DrainRequest, service: Annotated[JobService, Depends(get_job_service)]) -> DrainResponse:
        service.set_drain(request.enabled)
        return DrainResponse(drain_enabled=request.enabled)

    @app.get("/ops/drain", response_model=DrainResponse)
    def get_drain(service: Annotated[JobService, Depends(get_job_service)]) -> DrainResponse:
        return DrainResponse(drain_enabled=service.get_drain())

    return app


app = create_app()
