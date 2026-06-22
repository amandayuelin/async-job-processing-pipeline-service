from uuid import UUID

import pytest

from app.enums import JobStatus
from app.errors import DependencyUnavailableError
from app.kafka import FakeJobProducer
from app.schemas import JobCreate
from app.service import JobService


def create_job(service: JobService, handler: str = "echo", max_retries: int = 3) -> UUID:
    job, replay = service.submit_job(JobCreate(handler=handler, payload={"message": "hello"}, max_retries=max_retries))
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
    assert result.status == JobStatus.QUEUED
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
