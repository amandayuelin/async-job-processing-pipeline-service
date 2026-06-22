from functools import lru_cache
from typing import Annotated

from fastapi import Depends
from sqlalchemy.orm import Session

from app.config import Settings, get_settings
from app.database import get_db
from app.kafka import JobProducer, KafkaJobProducer
from app.repositories import SqlAlchemyJobRepository
from app.service import JobService


@lru_cache
def get_job_producer() -> JobProducer:
    return KafkaJobProducer(get_settings())


def get_job_service(
    db: Annotated[Session, Depends(get_db)],
    settings: Annotated[Settings, Depends(get_settings)],
) -> JobService:
    return JobService(SqlAlchemyJobRepository(db), get_job_producer(), settings)
