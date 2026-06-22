import pytest
from fastapi.testclient import TestClient

import app.main as main_module
from app.config import Settings
from app.dependencies import get_job_service
from app.kafka import FakeJobProducer
from app.main import create_app
from app.repositories import InMemoryJobRepository
from app.service import JobService


@pytest.fixture
def test_settings() -> Settings:
    return Settings(create_tables_on_startup=False, max_payload_bytes=1024)


@pytest.fixture
def repository() -> InMemoryJobRepository:
    return InMemoryJobRepository()


@pytest.fixture
def producer() -> FakeJobProducer:
    return FakeJobProducer()


@pytest.fixture
def service(repository: InMemoryJobRepository, producer: FakeJobProducer, test_settings: Settings) -> JobService:
    return JobService(repository, producer, test_settings)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, service: JobService, producer: FakeJobProducer, test_settings: Settings) -> TestClient:
    monkeypatch.setattr(main_module, "get_settings", lambda: test_settings)
    monkeypatch.setattr(main_module, "check_database_ready", lambda: True)
    monkeypatch.setattr(main_module, "get_job_producer", lambda: producer)

    app = create_app()
    app.dependency_overrides[get_job_service] = lambda: service
    return TestClient(app)
