from datetime import datetime, timezone
from uuid import UUID

import pytest

from app.core.enums import JobStatus
from app.core.errors import DependencyUnavailableError
from app.jobs.schemas import JobCreate
from app.jobs.service import JobService
from app.queue.kafka import FakeJobProducer


def create_job(
    service: JobService,
    handler: str = "echo",
    max_retries: int = 3,
    payload: dict | None = None,
    timeout_seconds: int = 30,
    recurring_cron: str | None = None,
) -> UUID:
    job, replay = service.submit_job(
        JobCreate(
            handler=handler,
            payload=payload or {"message": "hello"},
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            recurring_cron=recurring_cron,
        )
    )
    assert replay is False
    return job.id


def test_worker_processes_success(service: JobService) -> None:
    job_id = create_job(service)

    result = service.process_job(job_id, "worker-test")

    assert result is not None
    assert result.status == JobStatus.SUCCEEDED
    assert result.result == {"message": "hello"}
    assert result.attempt_count == 1


def test_duplicate_kafka_message_is_skipped_after_success(service: JobService) -> None:
    job_id = create_job(service)
    service.process_job(job_id, "worker-test")

    second = service.process_job(job_id, "worker-test")

    assert second is None


def test_worker_retries_transient_failure(service: JobService, producer: FakeJobProducer) -> None:
    job_id = create_job(service, handler="always_fail", max_retries=1)

    result = service.process_job(job_id, "worker-test")

    assert result is not None
    assert result.status == JobStatus.FAILED
    assert result.attempt_count == 1
    assert result.last_error == "hello"
    assert len(producer.published) == 1


def test_worker_dead_letters_after_retries(service: JobService, producer: FakeJobProducer) -> None:
    job_id = create_job(service, handler="always_fail", max_retries=0)

    result = service.process_job(job_id, "worker-test")

    assert result is not None
    assert result.status == JobStatus.DEAD_LETTERED
    assert result.attempt_count == 1
    assert producer.dead_letters[0]["job_id"] == str(job_id)


def test_worker_times_out_and_retries(service: JobService) -> None:
    job_id = create_job(service, handler="sleep", payload={"seconds": 2}, timeout_seconds=1)

    result = service.process_job(job_id, "worker-test")

    assert result is not None
    assert result.status == JobStatus.FAILED
    assert result.last_error == "job attempt timed out"


def test_successful_recurring_job_creates_next_run(service: JobService) -> None:
    job_id = create_job(service, recurring_cron="*/5 * * * *")

    result = service.process_job(job_id, "worker-test")
    jobs = service.list_jobs(None, None, 10, 0)

    assert result is not None
    assert result.status == JobStatus.SUCCEEDED
    recurring_jobs = [job for job in jobs if job.recurring_cron == "*/5 * * * *"]
    assert len(recurring_jobs) == 2
    assert any(job.status == JobStatus.QUEUED and job.run_at > datetime.now(timezone.utc) for job in recurring_jobs)


def test_drain_prevents_claim(service: JobService) -> None:
    job_id = create_job(service)
    service.set_drain(True)

    result = service.process_job(job_id, "worker-test")

    assert result is None
    assert service.get_job(job_id).status == JobStatus.QUEUED


def test_publish_due_jobs_republishes_queued_work(service: JobService, producer: FakeJobProducer) -> None:
    create_job(service)

    count = service.publish_due_jobs(limit=10)

    assert count == 1
    assert len(producer.published) == 2


def test_kafka_publish_failure_is_reported(service: JobService, producer: FakeJobProducer) -> None:
    producer.available = False

    with pytest.raises(DependencyUnavailableError):
        service.submit_job(JobCreate(handler="echo", payload={"message": "hello"}))
