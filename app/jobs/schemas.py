from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.core.enums import JobStatus
from app.jobs.scheduling import validate_cron_expression


class JobCreate(BaseModel):
    handler: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any]
    priority: int = Field(default=0, ge=0, le=10)
    max_retries: int = Field(default=3, ge=0, le=10)
    timeout_seconds: int = Field(default=30, ge=1, le=300)
    run_at: datetime | None = None
    recurring_cron: str | None = Field(default=None, min_length=9, max_length=100)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)

    @field_validator("handler")
    @classmethod
    def normalize_handler(cls, value: str) -> str:
        return value.strip()

    @field_validator("recurring_cron")
    @classmethod
    def validate_recurring_cron(cls, value: str | None) -> str | None:
        return validate_cron_expression(value) if value else None


class JobCreateResponse(BaseModel):
    id: UUID
    status: JobStatus
    idempotent_replay: bool


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    handler: str
    status: JobStatus
    priority: int
    attempt_count: int
    max_retries: int
    timeout_seconds: int
    run_at: datetime | None
    recurring_cron: str | None
    next_run_at: datetime
    last_error: str | None
    result: dict[str, Any] | None
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None


class JobListItem(BaseModel):
    id: UUID
    handler: str
    status: JobStatus
    attempt_count: int
    created_at: datetime


class JobListResponse(BaseModel):
    items: list[JobListItem]
    limit: int
    offset: int


class QueueDepthPriority(BaseModel):
    priority: int
    queued: int


class QueueDepthResponse(BaseModel):
    queued: int
    due: int
    running: int
    dead_lettered: int
    by_priority: list[QueueDepthPriority]


class DrainRequest(BaseModel):
    enabled: bool


class DrainResponse(BaseModel):
    drain_enabled: bool


class CancelResponse(BaseModel):
    id: UUID
    status: JobStatus


class MetricsResponse(BaseModel):
    job_success_count: int
    job_failure_count: int
    job_success_rate: float
    job_failure_rate: float
    retry_count: int
    dead_letter_count: int
    job_latency_p50_seconds: float
    job_latency_p95_seconds: float
    worker_utilization: float


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str
    details: dict[str, Any] = Field(default_factory=dict)


class ErrorResponse(BaseModel):
    error: ErrorBody
