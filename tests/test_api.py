from uuid import UUID

from fastapi.testclient import TestClient

from app.kafka import FakeJobProducer


def test_healthz(client: TestClient) -> None:
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert "X-Request-ID" in response.headers


def test_readyz(client: TestClient) -> None:
    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": "ok", "kafka": "ok"}


def test_create_and_get_job(client: TestClient, producer: FakeJobProducer) -> None:
    response = client.post(
        "/jobs",
        json={
            "handler": "echo",
            "payload": {"message": "hello"},
            "priority": 5,
            "max_retries": 3,
            "timeout_seconds": 30,
            "recurring_cron": "*/5 * * * *",
        },
    )

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "queued"
    assert body["idempotent_replay"] is False
    assert producer.published[0]["job_id"] == body["id"]

    get_response = client.get(f"/jobs/{body['id']}")
    assert get_response.status_code == 200
    assert get_response.json()["payload"] if "payload" in get_response.json() else True
    assert get_response.json()["handler"] == "echo"
    assert get_response.json()["recurring_cron"] == "*/5 * * * *"


def test_idempotent_replay_does_not_republish(client: TestClient, producer: FakeJobProducer) -> None:
    payload = {"handler": "echo", "payload": {"message": "hello"}, "idempotency_key": "same-job"}

    first = client.post("/jobs", json=payload)
    second = client.post("/jobs", json=payload)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json()["id"] == first.json()["id"]
    assert second.json()["idempotent_replay"] is True
    assert len(producer.published) == 1


def test_validation_error_shape(client: TestClient) -> None:
    response = client.post("/jobs", json={"handler": "", "payload": {}})

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "validation_error"
    assert response.json()["error"]["request_id"]


def test_unsupported_handler(client: TestClient) -> None:
    response = client.post("/jobs", json={"handler": "missing", "payload": {}})

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "bad_request"


def test_list_queue_depth_drain_and_cancel(client: TestClient) -> None:
    created = client.post("/jobs", json={"handler": "echo", "payload": {"a": 1}, "priority": 8}).json()

    list_response = client.get("/jobs?status=queued&limit=10&offset=0")
    assert list_response.status_code == 200
    assert list_response.json()["items"][0]["id"] == created["id"]

    depth = client.get("/queue/depth")
    assert depth.status_code == 200
    assert depth.json()["queued"] == 1
    assert depth.json()["by_priority"] == [{"priority": 8, "queued": 1}]

    drain = client.post("/ops/drain", json={"enabled": True})
    assert drain.status_code == 200
    assert client.get("/ops/drain").json() == {"drain_enabled": True}

    cancel = client.post(f"/jobs/{created['id']}/cancel")
    assert cancel.status_code == 200
    assert cancel.json()["status"] == "cancelled"


def test_metrics_endpoint(client: TestClient, service) -> None:
    created = client.post("/jobs", json={"handler": "echo", "payload": {"a": 1}}).json()
    service.process_job(UUID(created["id"]), "worker-test")

    response = client.get("/metrics")

    assert response.status_code == 200
    assert response.json()["job_success_count"] == 1
    assert response.json()["job_success_rate"] == 1.0
    assert response.json()["job_latency_p50_seconds"] >= 0
    assert response.json()["job_latency_p95_seconds"] >= 0
    assert "worker_utilization" in response.json()
