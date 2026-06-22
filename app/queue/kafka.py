from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol
from uuid import UUID

from kafka import KafkaAdminClient, KafkaProducer
from kafka.errors import KafkaError

from app.core.config import Settings, get_settings
from app.core.errors import DependencyUnavailableError


class JobProducer(Protocol):
    def publish_job(self, job_id: UUID, handler: str, priority: int, due_at: str, attempt: int = 0) -> None:
        ...

    def publish_dead_letter(self, job_id: UUID, handler: str, attempt: int) -> None:
        ...

    def ready(self) -> bool:
        ...


def topic_for_priority(settings: Settings, priority: int) -> str:
    if priority >= 8:
        return settings.kafka_submitted_high_topic
    if priority == 0:
        return settings.kafka_submitted_low_topic
    return settings.kafka_submitted_default_topic


class KafkaJobProducer:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        kwargs: dict[str, Any] = {
            "bootstrap_servers": self.settings.kafka_bootstrap_servers,
            "value_serializer": lambda value: json.dumps(value).encode("utf-8"),
            "key_serializer": lambda value: str(value).encode("utf-8"),
            "acks": "all",
            "retries": 3,
        }
        if self.settings.kafka_username and self.settings.kafka_password:
            kwargs.update(
                {
                    "security_protocol": self.settings.kafka_security_protocol,
                    "sasl_mechanism": self.settings.kafka_sasl_mechanism,
                    "sasl_plain_username": self.settings.kafka_username,
                    "sasl_plain_password": self.settings.kafka_password,
                }
            )
        self.producer = KafkaProducer(**kwargs)

    def publish_job(self, job_id: UUID, handler: str, priority: int, due_at: str, attempt: int = 0) -> None:
        topic = topic_for_priority(self.settings, priority)
        payload = {"job_id": str(job_id), "handler": handler, "attempt": attempt, "due_at": due_at}
        try:
            future = self.producer.send(topic, key=str(job_id), value=payload)
            future.get(timeout=10)
            self.producer.flush(timeout=10)
        except KafkaError as exc:
            raise DependencyUnavailableError("Kafka publish failed", {"job_id": str(job_id)}) from exc

    def publish_dead_letter(self, job_id: UUID, handler: str, attempt: int) -> None:
        payload = {"job_id": str(job_id), "handler": handler, "attempt": attempt}
        try:
            future = self.producer.send(self.settings.kafka_dead_letter_topic, key=str(job_id), value=payload)
            future.get(timeout=10)
            self.producer.flush(timeout=10)
        except KafkaError as exc:
            raise DependencyUnavailableError("Kafka dead-letter publish failed", {"job_id": str(job_id)}) from exc

    def ready(self) -> bool:
        try:
            admin = KafkaAdminClient(bootstrap_servers=self.settings.kafka_bootstrap_servers, request_timeout_ms=2000)
            admin.close()
            return True
        except Exception:
            return False


@dataclass
class FakeJobProducer:
    published: list[dict[str, Any]] = field(default_factory=list)
    dead_letters: list[dict[str, Any]] = field(default_factory=list)
    available: bool = True

    def publish_job(self, job_id: UUID, handler: str, priority: int, due_at: str, attempt: int = 0) -> None:
        if not self.available:
            raise DependencyUnavailableError("Kafka publish failed", {"job_id": str(job_id)})
        self.published.append(
            {"job_id": str(job_id), "handler": handler, "priority": priority, "due_at": due_at, "attempt": attempt}
        )

    def publish_dead_letter(self, job_id: UUID, handler: str, attempt: int) -> None:
        if not self.available:
            raise DependencyUnavailableError("Kafka dead-letter publish failed", {"job_id": str(job_id)})
        self.dead_letters.append({"job_id": str(job_id), "handler": handler, "attempt": attempt})

    def ready(self) -> bool:
        return self.available
