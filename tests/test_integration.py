import os
from uuid import UUID

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.config import Settings
from app.kafka import KafkaJobProducer
from app.models import Base
from app.repositories import SqlAlchemyJobRepository
from app.schemas import JobCreate
from app.service import JobService


pytestmark = pytest.mark.integration


def integration_enabled() -> bool:
    return os.getenv("RUN_INTEGRATION") == "1"


@pytest.mark.skipif(not integration_enabled(), reason="Set RUN_INTEGRATION=1 with PostgreSQL and Kafka to run")
def test_postgres_kafka_submission_round_trip() -> None:
    settings = Settings()
    engine = create_engine(settings.database_url, pool_pre_ping=True)
    Base.metadata.create_all(bind=engine)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

    with session_factory() as db:
        service = JobService(SqlAlchemyJobRepository(db), KafkaJobProducer(settings), settings)
        job, replay = service.submit_job(JobCreate(handler="echo", payload={"integration": True}))

        assert replay is False
        assert isinstance(job.id, UUID)
        assert service.get_job(job.id).status == job.status
