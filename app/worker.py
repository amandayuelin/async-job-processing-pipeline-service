from __future__ import annotations

import json
import logging
import signal
from uuid import UUID

from kafka import KafkaConsumer

from app.config import get_settings
from app.database import SessionLocal, create_tables
from app.kafka import KafkaJobProducer
from app.repositories import SqlAlchemyJobRepository
from app.service import JobService

logger = logging.getLogger(__name__)


class Worker:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.running = True
        self.producer = KafkaJobProducer(self.settings)

    def stop(self, *_args) -> None:
        self.running = False

    def run(self) -> None:
        logging.basicConfig(level=self.settings.log_level)
        create_tables()
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)

        topics = [
            self.settings.kafka_submitted_high_topic,
            self.settings.kafka_submitted_default_topic,
            self.settings.kafka_submitted_low_topic,
            self.settings.kafka_retry_topic,
        ]
        consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=self.settings.kafka_bootstrap_servers,
            group_id="job-worker",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            key_deserializer=lambda value: value.decode("utf-8") if value else None,
            auto_offset_reset="earliest",
        )

        logger.info("worker_started", extra={"worker_id": self.settings.worker_id, "topics": topics})
        try:
            while self.running:
                records = consumer.poll(timeout_ms=int(self.settings.worker_poll_interval_seconds * 1000))
                self._publish_due_jobs()
                messages = [message for partition_messages in records.values() for message in partition_messages]
                for message in sorted(messages, key=lambda item: self._topic_rank(item.topic)):
                    self._process_message(message.value)
                    consumer.commit()
        finally:
            consumer.close()
            logger.info("worker_stopped", extra={"worker_id": self.settings.worker_id})

    def _process_message(self, payload: dict) -> None:
        job_id = UUID(payload["job_id"])
        with SessionLocal() as db:
            service = JobService(SqlAlchemyJobRepository(db), self.producer, self.settings)
            result = service.process_job(job_id, self.settings.worker_id)
            logger.info(
                "job_message_processed",
                extra={"job_id": str(job_id), "status": result.status.value if result else "skipped"},
            )

    def _publish_due_jobs(self) -> None:
        with SessionLocal() as db:
            service = JobService(SqlAlchemyJobRepository(db), self.producer, self.settings)
            count = service.publish_due_jobs(self.settings.worker_batch_size)
            if count:
                logger.info("due_jobs_published", extra={"count": count})

    def _topic_rank(self, topic: str) -> int:
        ranks = {
            self.settings.kafka_submitted_high_topic: 0,
            self.settings.kafka_submitted_default_topic: 1,
            self.settings.kafka_retry_topic: 2,
            self.settings.kafka_submitted_low_topic: 3,
        }
        return ranks.get(topic, 99)


def main() -> None:
    Worker().run()


if __name__ == "__main__":
    main()
