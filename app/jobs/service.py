from __future__ import annotations

import json
import multiprocessing
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from app.core.config import Settings, get_settings
from app.core.enums import JobStatus
from app.core.errors import BadRequestError
from app.jobs.handlers import TransientJobError, ensure_supported_handler, get_handler
from app.jobs.repositories import JobMetrics, JobRecord, JobRepository, QueueDepth
from app.jobs.schemas import JobCreate
from app.queue.kafka import JobProducer


class JobService:
    def __init__(self, repository: JobRepository, producer: JobProducer, settings: Settings | None = None) -> None:
        self.repository = repository
        self.producer = producer
        self.settings = settings or get_settings()

    def submit_job(self, request: JobCreate) -> tuple[JobRecord, bool]:
        ensure_supported_handler(request.handler)
        self._validate_payload_size(request.payload)
        job, replay = self.repository.create_job(request)
        if not replay:
            self.producer.publish_job(job.id, job.handler, job.priority, job.next_run_at.isoformat(), job.attempt_count)
        return job, replay

    def get_job(self, job_id: UUID) -> JobRecord:
        return self.repository.get_job(job_id)

    def list_jobs(self, status: JobStatus | None, handler: str | None, limit: int, offset: int) -> list[JobRecord]:
        if handler:
            ensure_supported_handler(handler)
        return self.repository.list_jobs(status, handler, min(limit, self.settings.max_page_size), offset)

    def queue_depth(self) -> QueueDepth:
        return self.repository.queue_depth()

    def cancel_job(self, job_id: UUID) -> JobRecord:
        return self.repository.cancel_job(job_id)

    def set_drain(self, enabled: bool) -> None:
        self.repository.set_drain(enabled)

    def get_drain(self) -> bool:
        return self.repository.get_drain()

    def metrics(self) -> JobMetrics:
        return self.repository.metrics()

    def process_job(self, job_id: UUID, worker_id: str) -> JobRecord | None:
        job = self.repository.claim_job(job_id, worker_id)
        if not job:
            return None
        handler = get_handler(job.handler)
        try:
            result = self._run_with_timeout(handler, job.payload, job.timeout_seconds)
            succeeded = self.repository.mark_succeeded(job.id, result)
            self.repository.create_next_recurring_job(succeeded)
            return succeeded
        except Exception as exc:
            error = str(exc) or exc.__class__.__name__
            backoff = self._backoff_seconds(job.attempt_count)
            updated = self.repository.mark_failed_attempt(job.id, error, backoff)
            if updated.status == JobStatus.DEAD_LETTERED:
                self.producer.publish_dead_letter(updated.id, updated.handler, updated.attempt_count)
            return updated

    def publish_due_jobs(self, limit: int) -> int:
        jobs = self.repository.due_jobs(limit)
        for job in jobs:
            self.producer.publish_job(job.id, job.handler, job.priority, job.next_run_at.isoformat(), job.attempt_count)
        return len(jobs)

    def _validate_payload_size(self, payload: dict[str, Any]) -> None:
        size = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        if size > self.settings.max_payload_bytes:
            raise BadRequestError("Payload too large", {"max_payload_bytes": self.settings.max_payload_bytes})

    @staticmethod
    def _backoff_seconds(attempt_count: int) -> int:
        return min(2 ** max(attempt_count - 1, 0), 300)

    @staticmethod
    def _run_with_timeout(handler: Any, payload: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
        context = multiprocessing.get_context("fork")
        queue = context.Queue()
        process = context.Process(target=_handler_process, args=(handler, payload, queue))
        process.start()
        process.join(timeout_seconds)
        if process.is_alive():
            process.terminate()
            process.join()
            raise TransientJobError("job attempt timed out")
        if queue.empty():
            raise TransientJobError("job attempt failed without a result")
        status, value = queue.get()
        if status == "ok":
            return value
        raise TransientJobError(value)


def success_failure_rates(metrics: JobMetrics) -> tuple[float, float]:
    total = metrics.job_success_count + metrics.job_failure_count + metrics.dead_letter_count
    if total == 0:
        return 0.0, 0.0
    success_rate = metrics.job_success_count / total
    failure_rate = (metrics.job_failure_count + metrics.dead_letter_count) / total
    return success_rate, failure_rate


def parse_job_id(value: str) -> UUID:
    return UUID(value)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _handler_process(handler: Any, payload: dict[str, Any], queue: Any) -> None:
    try:
        queue.put(("ok", handler(payload)))
    except Exception as exc:
        queue.put(("error", str(exc) or exc.__class__.__name__))
