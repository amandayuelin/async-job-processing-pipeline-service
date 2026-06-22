from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Protocol
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.enums import AttemptStatus, JobStatus
from app.errors import ConflictError, NotFoundError
from app.models import Job, JobAttempt, OpsSetting
from app.schemas import JobCreate


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_aware(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value


@dataclass
class JobRecord:
    id: UUID
    handler: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    max_retries: int
    timeout_seconds: int
    attempt_count: int
    next_run_at: datetime
    run_at: datetime | None
    locked_by: str | None
    locked_at: datetime | None
    kafka_message_key: str | None
    last_error: str | None
    result: dict[str, Any] | None
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime
    processed_at: datetime | None


@dataclass
class QueueDepth:
    queued: int
    due: int
    running: int
    dead_lettered: int
    by_priority: list[dict[str, int]]


@dataclass
class JobMetrics:
    job_success_count: int
    job_failure_count: int
    retry_count: int
    dead_letter_count: int


class JobRepository(Protocol):
    def create_job(self, request: JobCreate) -> tuple[JobRecord, bool]:
        ...

    def get_job(self, job_id: UUID) -> JobRecord:
        ...

    def list_jobs(self, status: JobStatus | None, handler: str | None, limit: int, offset: int) -> list[JobRecord]:
        ...

    def due_jobs(self, limit: int) -> list[JobRecord]:
        ...

    def queue_depth(self) -> QueueDepth:
        ...

    def metrics(self) -> JobMetrics:
        ...

    def claim_job(self, job_id: UUID, worker_id: str) -> JobRecord | None:
        ...

    def mark_succeeded(self, job_id: UUID, result: dict[str, Any]) -> JobRecord:
        ...

    def mark_failed_attempt(self, job_id: UUID, error: str, backoff_seconds: int) -> JobRecord:
        ...

    def cancel_job(self, job_id: UUID) -> JobRecord:
        ...

    def set_drain(self, enabled: bool) -> None:
        ...

    def get_drain(self) -> bool:
        ...


def _record_from_model(job: Job) -> JobRecord:
    return JobRecord(
        id=UUID(job.id),
        handler=job.handler,
        payload=job.payload,
        status=JobStatus(job.status),
        priority=job.priority,
        max_retries=job.max_retries,
        timeout_seconds=job.timeout_seconds,
        attempt_count=job.attempt_count,
        next_run_at=ensure_aware(job.next_run_at) or utc_now(),
        run_at=ensure_aware(job.run_at),
        locked_by=job.locked_by,
        locked_at=ensure_aware(job.locked_at),
        kafka_message_key=job.kafka_message_key,
        last_error=job.last_error,
        result=job.result,
        idempotency_key=job.idempotency_key,
        created_at=ensure_aware(job.created_at) or utc_now(),
        updated_at=ensure_aware(job.updated_at) or utc_now(),
        processed_at=ensure_aware(job.processed_at),
    )


class SqlAlchemyJobRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create_job(self, request: JobCreate) -> tuple[JobRecord, bool]:
        if request.idempotency_key:
            existing = self.db.scalar(select(Job).where(Job.idempotency_key == request.idempotency_key))
            if existing:
                return _record_from_model(existing), True

        now = utc_now()
        run_at = ensure_aware(request.run_at)
        next_run_at = run_at if run_at and run_at > now else now
        job_id = uuid4()
        job = Job(
            id=str(job_id),
            handler=request.handler,
            payload=request.payload,
            status=JobStatus.QUEUED.value,
            priority=request.priority,
            max_retries=request.max_retries,
            timeout_seconds=request.timeout_seconds,
            attempt_count=0,
            next_run_at=next_run_at,
            run_at=run_at,
            kafka_message_key=str(job_id),
            idempotency_key=request.idempotency_key,
            created_at=now,
            updated_at=now,
        )
        self.db.add(job)
        try:
            self.db.commit()
        except IntegrityError:
            self.db.rollback()
            if request.idempotency_key:
                existing = self.db.scalar(select(Job).where(Job.idempotency_key == request.idempotency_key))
                if existing:
                    return _record_from_model(existing), True
            raise
        self.db.refresh(job)
        return _record_from_model(job), False

    def get_job(self, job_id: UUID) -> JobRecord:
        job = self.db.get(Job, str(job_id))
        if not job:
            raise NotFoundError("Job not found")
        return _record_from_model(job)

    def list_jobs(self, status: JobStatus | None, handler: str | None, limit: int, offset: int) -> list[JobRecord]:
        query = select(Job).order_by(Job.created_at.desc()).limit(limit).offset(offset)
        if status:
            query = query.where(Job.status == status.value)
        if handler:
            query = query.where(Job.handler == handler)
        return [_record_from_model(job) for job in self.db.scalars(query)]

    def due_jobs(self, limit: int) -> list[JobRecord]:
        query = (
            select(Job)
            .where(Job.status == JobStatus.QUEUED.value, Job.next_run_at <= utc_now())
            .order_by(Job.priority.desc(), Job.created_at.asc())
            .limit(limit)
        )
        return [_record_from_model(job) for job in self.db.scalars(query)]

    def queue_depth(self) -> QueueDepth:
        now = utc_now()
        queued = self.db.scalar(select(func.count()).select_from(Job).where(Job.status == JobStatus.QUEUED.value)) or 0
        due = self.db.scalar(
            select(func.count()).select_from(Job).where(Job.status == JobStatus.QUEUED.value, Job.next_run_at <= now)
        ) or 0
        running = self.db.scalar(select(func.count()).select_from(Job).where(Job.status == JobStatus.RUNNING.value)) or 0
        dead = self.db.scalar(
            select(func.count()).select_from(Job).where(Job.status == JobStatus.DEAD_LETTERED.value)
        ) or 0
        rows = self.db.execute(
            select(Job.priority, func.count()).where(Job.status == JobStatus.QUEUED.value).group_by(Job.priority)
        ).all()
        return QueueDepth(queued, due, running, dead, [{"priority": row[0], "queued": row[1]} for row in rows])

    def metrics(self) -> JobMetrics:
        success = self.db.scalar(select(func.count()).select_from(Job).where(Job.status == JobStatus.SUCCEEDED.value)) or 0
        failed = self.db.scalar(select(func.count()).select_from(Job).where(Job.status == JobStatus.FAILED.value)) or 0
        dead = self.db.scalar(
            select(func.count()).select_from(Job).where(Job.status == JobStatus.DEAD_LETTERED.value)
        ) or 0
        retries = self.db.scalar(select(func.coalesce(func.sum(Job.attempt_count - 1), 0)).select_from(Job)) or 0
        return JobMetrics(success, failed, max(int(retries), 0), dead)

    def claim_job(self, job_id: UUID, worker_id: str) -> JobRecord | None:
        if self.get_drain():
            return None
        job = self.db.get(Job, str(job_id))
        now = utc_now()
        if not job or job.status != JobStatus.QUEUED.value or ensure_aware(job.next_run_at) > now:
            return None
        job.status = JobStatus.RUNNING.value
        job.locked_by = worker_id
        job.locked_at = now
        job.attempt_count += 1
        job.updated_at = now
        attempt = JobAttempt(
            id=str(uuid4()),
            job_id=job.id,
            attempt_number=job.attempt_count,
            status=AttemptStatus.RUNNING.value,
            started_at=now,
            worker_id=worker_id,
        )
        self.db.add(attempt)
        self.db.commit()
        self.db.refresh(job)
        return _record_from_model(job)

    def mark_succeeded(self, job_id: UUID, result: dict[str, Any]) -> JobRecord:
        job = self._get_model(job_id)
        now = utc_now()
        job.status = JobStatus.SUCCEEDED.value
        job.result = result
        job.locked_by = None
        job.locked_at = None
        job.processed_at = now
        job.updated_at = now
        self._finish_attempt(job, AttemptStatus.SUCCEEDED, None)
        self.db.commit()
        self.db.refresh(job)
        return _record_from_model(job)

    def mark_failed_attempt(self, job_id: UUID, error: str, backoff_seconds: int) -> JobRecord:
        job = self._get_model(job_id)
        now = utc_now()
        exhausted = job.attempt_count > job.max_retries
        job.status = JobStatus.DEAD_LETTERED.value if exhausted else JobStatus.QUEUED.value
        job.last_error = error
        job.locked_by = None
        job.locked_at = None
        job.next_run_at = now + timedelta(seconds=backoff_seconds)
        job.updated_at = now
        if exhausted:
            job.processed_at = now
        self._finish_attempt(job, AttemptStatus.FAILED, error)
        self.db.commit()
        self.db.refresh(job)
        return _record_from_model(job)

    def cancel_job(self, job_id: UUID) -> JobRecord:
        job = self._get_model(job_id)
        if job.status != JobStatus.QUEUED.value:
            raise ConflictError("Only queued jobs can be cancelled", {"status": job.status})
        now = utc_now()
        job.status = JobStatus.CANCELLED.value
        job.updated_at = now
        job.processed_at = now
        self.db.commit()
        self.db.refresh(job)
        return _record_from_model(job)

    def set_drain(self, enabled: bool) -> None:
        setting = self.db.get(OpsSetting, "drain_enabled")
        if setting:
            setting.value = {"enabled": enabled}
            setting.updated_at = utc_now()
        else:
            self.db.add(OpsSetting(key="drain_enabled", value={"enabled": enabled}, updated_at=utc_now()))
        self.db.commit()

    def get_drain(self) -> bool:
        setting = self.db.get(OpsSetting, "drain_enabled")
        return bool(setting and setting.value.get("enabled") is True)

    def _get_model(self, job_id: UUID) -> Job:
        job = self.db.get(Job, str(job_id))
        if not job:
            raise NotFoundError("Job not found")
        return job

    def _finish_attempt(self, job: Job, status: AttemptStatus, error: str | None) -> None:
        attempt = self.db.scalar(
            select(JobAttempt).where(JobAttempt.job_id == job.id, JobAttempt.attempt_number == job.attempt_count)
        )
        if attempt:
            attempt.status = status.value
            attempt.error = error
            attempt.finished_at = utc_now()


class InMemoryJobRepository:
    def __init__(self) -> None:
        self.jobs: dict[UUID, JobRecord] = {}
        self.idempotency: dict[str, UUID] = {}
        self.drain_enabled = False
        self.lock = Lock()

    def create_job(self, request: JobCreate) -> tuple[JobRecord, bool]:
        with self.lock:
            if request.idempotency_key and request.idempotency_key in self.idempotency:
                return self.jobs[self.idempotency[request.idempotency_key]], True
            now = utc_now()
            run_at = ensure_aware(request.run_at)
            next_run_at = run_at if run_at and run_at > now else now
            job_id = uuid4()
            job = JobRecord(
                id=job_id,
                handler=request.handler,
                payload=request.payload,
                status=JobStatus.QUEUED,
                priority=request.priority,
                max_retries=request.max_retries,
                timeout_seconds=request.timeout_seconds,
                attempt_count=0,
                next_run_at=next_run_at,
                run_at=run_at,
                locked_by=None,
                locked_at=None,
                kafka_message_key=str(job_id),
                last_error=None,
                result=None,
                idempotency_key=request.idempotency_key,
                created_at=now,
                updated_at=now,
                processed_at=None,
            )
            self.jobs[job_id] = job
            if request.idempotency_key:
                self.idempotency[request.idempotency_key] = job_id
            return job, False

    def get_job(self, job_id: UUID) -> JobRecord:
        try:
            return self.jobs[job_id]
        except KeyError as exc:
            raise NotFoundError("Job not found") from exc

    def list_jobs(self, status: JobStatus | None, handler: str | None, limit: int, offset: int) -> list[JobRecord]:
        jobs = sorted(self.jobs.values(), key=lambda job: job.created_at, reverse=True)
        if status:
            jobs = [job for job in jobs if job.status == status]
        if handler:
            jobs = [job for job in jobs if job.handler == handler]
        return jobs[offset : offset + limit]

    def due_jobs(self, limit: int) -> list[JobRecord]:
        now = utc_now()
        jobs = [
            job
            for job in self.jobs.values()
            if job.status == JobStatus.QUEUED and job.next_run_at <= now
        ]
        jobs = sorted(jobs, key=lambda job: (-job.priority, job.created_at))
        return jobs[:limit]

    def queue_depth(self) -> QueueDepth:
        now = utc_now()
        queued_jobs = [job for job in self.jobs.values() if job.status == JobStatus.QUEUED]
        priorities: dict[int, int] = {}
        for job in queued_jobs:
            priorities[job.priority] = priorities.get(job.priority, 0) + 1
        return QueueDepth(
            queued=len(queued_jobs),
            due=sum(1 for job in queued_jobs if job.next_run_at <= now),
            running=sum(1 for job in self.jobs.values() if job.status == JobStatus.RUNNING),
            dead_lettered=sum(1 for job in self.jobs.values() if job.status == JobStatus.DEAD_LETTERED),
            by_priority=[{"priority": priority, "queued": count} for priority, count in sorted(priorities.items())],
        )

    def metrics(self) -> JobMetrics:
        success = sum(1 for job in self.jobs.values() if job.status == JobStatus.SUCCEEDED)
        failed = sum(1 for job in self.jobs.values() if job.status == JobStatus.FAILED)
        dead = sum(1 for job in self.jobs.values() if job.status == JobStatus.DEAD_LETTERED)
        retries = sum(max(job.attempt_count - 1, 0) for job in self.jobs.values())
        return JobMetrics(success, failed, retries, dead)

    def claim_job(self, job_id: UUID, worker_id: str) -> JobRecord | None:
        with self.lock:
            if self.drain_enabled:
                return None
            job = self.jobs.get(job_id)
            now = utc_now()
            if not job or job.status != JobStatus.QUEUED or job.next_run_at > now:
                return None
            updated = job.__dict__ | {
                "status": JobStatus.RUNNING,
                "locked_by": worker_id,
                "locked_at": now,
                "attempt_count": job.attempt_count + 1,
                "updated_at": now,
            }
            self.jobs[job_id] = JobRecord(**updated)
            return self.jobs[job_id]

    def mark_succeeded(self, job_id: UUID, result: dict[str, Any]) -> JobRecord:
        with self.lock:
            job = self.get_job(job_id)
            now = utc_now()
            updated = job.__dict__ | {
                "status": JobStatus.SUCCEEDED,
                "result": result,
                "locked_by": None,
                "locked_at": None,
                "updated_at": now,
                "processed_at": now,
            }
            self.jobs[job_id] = JobRecord(**updated)
            return self.jobs[job_id]

    def mark_failed_attempt(self, job_id: UUID, error: str, backoff_seconds: int) -> JobRecord:
        with self.lock:
            job = self.get_job(job_id)
            now = utc_now()
            exhausted = job.attempt_count > job.max_retries
            updated = job.__dict__ | {
                "status": JobStatus.DEAD_LETTERED if exhausted else JobStatus.QUEUED,
                "last_error": error,
                "locked_by": None,
                "locked_at": None,
                "next_run_at": now + timedelta(seconds=backoff_seconds),
                "updated_at": now,
                "processed_at": now if exhausted else None,
            }
            self.jobs[job_id] = JobRecord(**updated)
            return self.jobs[job_id]

    def cancel_job(self, job_id: UUID) -> JobRecord:
        with self.lock:
            job = self.get_job(job_id)
            if job.status != JobStatus.QUEUED:
                raise ConflictError("Only queued jobs can be cancelled", {"status": job.status.value})
            now = utc_now()
            updated = job.__dict__ | {"status": JobStatus.CANCELLED, "updated_at": now, "processed_at": now}
            self.jobs[job_id] = JobRecord(**updated)
            return self.jobs[job_id]

    def set_drain(self, enabled: bool) -> None:
        self.drain_enabled = enabled

    def get_drain(self) -> bool:
        return self.drain_enabled
