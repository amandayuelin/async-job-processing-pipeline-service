# Code Review Walkthrough

This file is meant for the live code review. It explains what to show, why the system is shaped this way, and the trade-offs behind the main technical choices.

## One-Minute System Summary

The service accepts asynchronous jobs through a REST API, persists every job in PostgreSQL, publishes work to Kafka, and lets independently scalable workers execute jobs with retry, timeout, cancellation, dead-letter, and visibility semantics.

The central design choice is that PostgreSQL is the source of truth and Kafka is the delivery mechanism. That gives durable lifecycle visibility even when Kafka messages are duplicated, delayed, or consumed after a retry window has changed.

## Walkthrough Path

Start with the API boundary:

- `app/main.py` shows the REST endpoints, request ID middleware, structured error handling, health checks, readiness checks, job listing, queue depth, metrics, cancellation, and drain mode.
- `app/jobs/schemas.py` shows the API contract and validation rules for payload size, retry count, timeout, priority, delayed jobs, and recurring cron.
- `app/api/dependencies.py` wires the request-scoped repository and Kafka producer into the service layer.

Then move to business logic:

- `app/jobs/service.py` is the orchestration layer. It validates handlers, creates jobs, publishes Kafka messages, claims work, runs handlers with timeout enforcement, applies retry backoff, publishes dead-letter events, and republishes due retry jobs.
- `app/jobs/repositories.py` owns persistence and state transitions. This is where idempotency, queue depth, metrics, drain mode, recurring jobs, cancellation, and attempt records are implemented.
- `app/db/models.py` defines the durable job and attempt schema.

Then show the asynchronous path:

- `app/queue/kafka.py` maps priorities to Kafka topics and publishes job or dead-letter messages with `acks=all`.
- `app/worker.py` consumes priority and retry topics, processes messages through the same `JobService`, and commits offsets only after processing.
- `app/jobs/handlers.py` is intentionally small so handler execution is pluggable and testable.

Close with verification and deployment:

- `tests/test_api.py` covers the public API behavior.
- `tests/test_service.py` covers retry, timeout, dead-letter, cancellation, idempotency, metrics, and recurring behavior.
- `tests/test_integration.py` is opt-in for live PostgreSQL/Kafka.
- `infra/terraform/` provisions the self-managed data Droplet.
- `infra/app.yaml` and `scripts/deploy.sh` deploy API and worker components to DigitalOcean App Platform.

## Why These Choices

PostgreSQL as source of truth:

- The API can return job status, attempts, errors, results, retry timing, queue depth, and metrics without depending on Kafka retention or consumer state.
- Duplicate Kafka messages are safe because workers re-read the current job status before running work.
- This makes cancellation and drain mode enforceable through database state.

Kafka for delivery:

- Kafka decouples API latency from handler execution.
- Worker replicas can scale independently from API replicas.
- Topic partitioning gives a clear path to higher throughput.
- Kafka is still treated as at-least-once delivery, so correctness lives in the database state machine.

Explicit job state machine:

- `queued`, `running`, `succeeded`, `failed`, `dead_lettered`, and `cancelled` are visible client states.
- A retry-waiting job remains `failed` with a future `next_run_at`, which makes failures visible instead of hiding them as queued work.
- Exhausted retries move to `dead_lettered`, and dead-letter events are mirrored in Kafka.

Priority topics:

- Kafka does not provide global priority ordering.
- The MVP maps priority `8-10` to high, `1-7` to default, and `0` to low.
- Workers sort polled messages by topic rank, which gives practical priority preference without pretending there is strict global ordering.

Process-based timeouts:

- A thread timeout cannot safely stop arbitrary blocking handler code in Python.
- Running the handler in a child process lets the worker terminate a timed-out attempt.
- The trade-off is more overhead per attempt and a Unix-oriented `fork` assumption for this MVP.

Self-managed PostgreSQL and Kafka for the MVP:

- The problem requires DigitalOcean deployment, independent worker scaling, durability, retries, timeout, DLQ, visibility, and tests.
- It does not require DigitalOcean Managed PostgreSQL or Managed Kafka.
- Self-managed data services make the demo faster to provision in an interview setting.
- The production path should migrate PostgreSQL and Kafka to managed services, tighten network controls, add backups, and remove public data-plane access.

## Main Trade-Offs

Database commit plus Kafka publish is not fully atomic.

The current implementation inserts the job, commits it, then publishes to Kafka. If Kafka publish fails after the database commit, the API returns a dependency error and the durable row remains. The worker-side due-job publisher can republish eligible jobs, but a production-grade version should use a transactional outbox table and a relay process so database writes and publish intent are committed together.

At-least-once processing instead of exactly-once processing.

Kafka can redeliver messages and workers can crash after running a handler but before committing an offset. The system handles this by claiming jobs through PostgreSQL state and skipping terminal or not-yet-due jobs. True exactly-once handler side effects require idempotent handlers or external idempotency keys at the handler boundary.

Priority is best-effort.

Priority topics are simple and observable, but they do not give strict ordering across partitions or workers. For hard priority guarantees, a database-backed priority queue or a more specialized scheduler would be easier to reason about than raw Kafka topic ordering.

Cron support is intentionally small.

The MVP supports five-field cron expressions using `*`, `*/n`, and exact numeric values. That is enough to demonstrate recurring jobs without bringing in a scheduler dependency. Production should use a mature cron parser and store schedule metadata separately from individual job attempts.

Metrics are application-level, not a full observability stack.

`/metrics` exposes success and failure counts/rates, retries, dead letters, latency percentiles, and worker utilization. This is useful for the review and smoke testing, but production should export Prometheus-compatible metrics, distributed traces, Kafka consumer lag, and database health dashboards.

Deployment favors one-shot demo speed.

Terraform provisions the data host, while App Spec deploys the API and worker. This keeps manual steps low and shows infrastructure ownership. For production, the data plane should be managed or private-networked, secrets should move to a secret manager, and Terraform state should use a remote backend.

## What I Would Improve Next

- Add a transactional outbox and relay for reliable DB-to-Kafka publishing.
- Move PostgreSQL and Kafka to managed services or a private network with tighter firewall rules.
- Add Prometheus metrics, structured JSON logs, and tracing.
- Add Alembic migrations instead of startup table creation.
- Add handler-level idempotency contracts for jobs with external side effects.
- Add integration tests that run the API, worker, PostgreSQL, and Kafka together in CI.
- Add retention cleanup for old terminal jobs and attempts.

## Review Sound Bites

- "The database owns correctness; Kafka owns delivery."
- "Workers are allowed to see duplicate messages because state transitions are guarded in PostgreSQL."
- "Retry-waiting jobs are intentionally visible as failed so clients can understand that work has failed but is not exhausted."
- "The MVP uses self-managed data services for deployment speed, but the production migration path is clear."
- "I chose focused tests around state transitions because that is where asynchronous systems usually break."
