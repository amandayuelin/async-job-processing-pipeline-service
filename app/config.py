from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = "local"
    log_level: str = "INFO"
    port: int = 8000

    database_url: str = "postgresql+psycopg://postgres:postgres@localhost:5432/jobs"
    create_tables_on_startup: bool = True

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_submitted_high_topic: str = "jobs.submitted.high"
    kafka_submitted_default_topic: str = "jobs.submitted.default"
    kafka_submitted_low_topic: str = "jobs.submitted.low"
    kafka_retry_topic: str = "jobs.retry"
    kafka_dead_letter_topic: str = "jobs.dead_lettered"
    kafka_username: str | None = None
    kafka_password: str | None = None

    max_page_size: int = 100
    max_payload_bytes: int = 64 * 1024
    worker_id: str = "worker-local"
    worker_poll_interval_seconds: float = 1.0
    worker_batch_size: int = 10
    stale_lock_seconds: int = 300

    request_id_header: str = Field(default="X-Request-ID")


@lru_cache
def get_settings() -> Settings:
    return Settings()
