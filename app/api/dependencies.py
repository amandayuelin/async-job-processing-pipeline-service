from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from sqlalchemy.orm import Session

from app.core.config import Settings, get_settings
from app.db.session import get_db
from app.jobs.repositories import SqlAlchemyJobRepository
from app.jobs.service import JobService
from app.queue.kafka import JobProducer, KafkaJobProducer


@lru_cache
def get_job_producer() -> JobProducer:
    return KafkaJobProducer(get_settings())


def get_job_service(
    db: Annotated[Session, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> JobService:
    return JobService(SqlAlchemyJobRepository(db), get_job_producer(), settings)
