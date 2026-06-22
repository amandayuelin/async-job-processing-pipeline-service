# Async Job Processing Pipeline Service

Production-candidate FastAPI service for asynchronous job submission, execution, retry, dead-lettering, and lifecycle visibility.

## Architecture

- FastAPI API accepts jobs and returns a job ID immediately.
- PostgreSQL stores authoritative job state, attempts, idempotency keys, results, drain mode, and dead-letter state.
- Kafka transports submitted/retry/dead-letter job events between API and workers.
- Worker processes consume Kafka messages, verify PostgreSQL state, execute handlers, and update results.
- DigitalOcean App Platform runs the API and worker components. PostgreSQL and Kafka can be self-maintained for the timed MVP.

## Local Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
docker compose up -d db kafka
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Run a worker in a second shell:

```bash
python -m app.worker
```

## Tests

```bash
pytest
```

Unit and API tests use an in-memory repository and fake Kafka producer so the fast test suite does not require live PostgreSQL or Kafka. Integration testing can use `docker compose`.

## API

- `GET /healthz`
- `GET /readyz`
- `POST /jobs`
- `GET /jobs/{job_id}`
- `GET /jobs`
- `GET /queue/depth`
- `GET /metrics`
- `POST /jobs/{job_id}/cancel`
- `POST /ops/drain`
- `GET /ops/drain`

Example job submission:

```bash
curl -X POST http://localhost:8000/jobs \
  -H "Content-Type: application/json" \
  -d '{"handler":"echo","payload":{"message":"hello"},"priority":5,"max_retries":3,"timeout_seconds":30}'
```

Supported MVP handlers:

- `echo`: returns the submitted payload.
- `always_fail`: raises a transient error for retry/dead-letter testing.
- `fail_once`: test helper handler.

## Configuration

Important runtime variables:

- `DATABASE_URL`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_SUBMITTED_HIGH_TOPIC`
- `KAFKA_SUBMITTED_DEFAULT_TOPIC`
- `KAFKA_SUBMITTED_LOW_TOPIC`
- `KAFKA_RETRY_TOPIC`
- `KAFKA_DEAD_LETTER_TOPIC`
- `MAX_PAYLOAD_BYTES`
- `MAX_PAGE_SIZE`
- `WORKER_ID`

Deployment-only variables:

- `DIGITALOCEAN_ACCESS_TOKEN`
- `DO_APP_NAME`
- `DO_REGION`
- `GITHUB_REPO`
- `GITHUB_BRANCH`

Never commit real tokens or credentials.

## Deployment

The repo includes a DigitalOcean App Platform spec template at `infra/app.yaml` and a one-shot deployment script at `scripts/deploy.sh`.

```bash
export DIGITALOCEAN_ACCESS_TOKEN=...
export DATABASE_URL=...
export KAFKA_BOOTSTRAP_SERVERS=...
export GITHUB_REPO=owner/repo
./scripts/deploy.sh
```

Pause before running deployment until the DigitalOcean token and endpoint values are available.

Smoke test after deploy:

```bash
./scripts/smoke.sh https://<app-url>
```

## High Load Notes

- Scale API replicas and worker replicas independently on App Platform.
- Increase Kafka partitions for more worker parallelism.
- Keep PostgreSQL indexes on job status, retry timing, and created time.
- Keep handlers idempotent because Kafka and worker processing are at-least-once.
- Add transactional outbox before depending on strict DB-to-Kafka publish recovery.
- Add managed PostgreSQL/Kafka, metrics, tracing, rate limiting, and retention cleanup for production hardening.
