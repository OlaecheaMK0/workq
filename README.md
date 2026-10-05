# workq: durable background jobs

[![workq checks](https://github.com/OlaecheaMK0/workq/actions/workflows/ci.yml/badge.svg)](https://github.com/OlaecheaMK0/workq/actions/workflows/ci.yml)

A small, durable background job system built with Go and PostgreSQL. Multiple workers claim ready jobs without waiting on one another's row locks; leases let another worker recover abandoned work. A browser dashboard makes retries, success, and dead letters visible.

The included `report` handler sums synthetic numbers and stores one report per job. It does not send messages, charge money, fetch URLs, or write to third-party systems.

The local HTTP demo verified a report succeeding on attempt one, a retry example succeeding on attempt three, and a dead letter stopping at attempt three. Real PostgreSQL tests also verify lease recovery and durable replay protection. See [validation](VALIDATION.md).

## Run the local deployment

Requires Docker with Compose. The example database credentials are deliberately local demo values, and both published ports bind to loopback.

```sh
docker compose up -d --build --wait
docker compose ps
```

Open [http://localhost:18380](http://localhost:18380). Submit a normal report, a retry example, and a dead-letter example from the dashboard. State counts update every two seconds. PostgreSQL persists in the named `workq-data` volume. To stop the stack while preserving it:

```sh
docker compose down
```

Two worker goroutines run by default. Additional worker processes use the same queue:

```sh
docker compose up -d --scale worker=3
```

The Go image runs as UID 10001 with a read-only filesystem, no Linux capabilities, and no privilege escalation. The supplied Compose configuration runs a local demo.

## Submit and inspect jobs

```sh
curl -i http://localhost:18380/v1/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-report-001' \
  -d '{"kind":"report","payload":{"name":"Example","items":[10,20,30],"fail_until_attempt":2},"max_attempts":3}'

curl 'http://localhost:18380/v1/jobs?state=succeeded'
curl http://localhost:18380/v1/stats
```

The first submission returns `201`, a `Location` header, and a job ID. Repeating the same key and request returns the existing job with `200`; changing that request while reusing its key returns `409`. Object-key order is normalized for this comparison. Array order, numeric representation, kind, and the attempt limit affect identity. Omitting `max_attempts` gives a cap of three. Keys are retained for the lifetime of their job.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/` | Interactive local dashboard |
| GET | `/healthz` | Process liveness |
| GET | `/readyz` | Database readiness |
| POST | `/v1/jobs` | Submit one report job with `Idempotency-Key` |
| GET | `/v1/jobs/{uuid}` | Status, attempts, last error, and successful result |
| GET | `/v1/jobs?state=dead&limit=20` | Recent jobs; optional state and 1–100 limit |
| GET | `/v1/stats` | Counts grouped by state |

The API accepts at most 64 KiB per request and rejects unknown fields. Reports require a name of 1–80 bytes and 1–1000 integers in the range ±1,000,000. `delay_ms` is capped at 10 seconds and `fail_until_attempt` at 10. These two fields make failure and slow-processing behavior reproducible.

## What delivery means

```mermaid
stateDiagram-v2
  [*] --> queued: idempotent submission
  queued --> running: claim + new lease token
  running --> running: heartbeat
  running --> succeeded: result + fenced acknowledgment
  running --> queued: failure or expired lease / attempts left
  running --> dead: failure or expired lease / attempt cap reached
```

- Claims use one `UPDATE` with a `FOR UPDATE SKIP LOCKED` candidate query. Concurrent consumers can claim different jobs atomically. PostgreSQL specifically describes this locking mode as useful for queue-like tables; it is not a general consistent-read mechanism. [PostgreSQL SELECT documentation](https://www.postgresql.org/docs/17/sql-select.html)
- Every claim increments `attempts` and creates a random lease token. A heartbeat extends the lease every third of its duration. Complete, fail, and heartbeat updates all require the current token and an unexpired lease. A stale worker cannot acknowledge a newer owner's attempt.
- Failed jobs use exponential backoff with equal jitter: the delay is between half and all of the current cap, starting at 500 ms and capped at 30 seconds. A lease recovered after a crash is immediately eligible if attempts remain; this is distinct from a handler failure retry.
- A worker that loses its lease cancels its handler and leaves acknowledgment to recovery. Graceful shutdown also leaves unfinished work for lease recovery. Every claim counts toward the attempt cap, including a process crash before executing the handler.
- **Delivery is at least once.** A crash can happen after an effect but before queue acknowledgment. The report handler uses a unique `job_id` in `report_results` so replay returns the previously stored result. The handler and acknowledgment intentionally use separate transactions to demonstrate this crash window. There is no general exactly-once guarantee for external side effects.

## Develop and test

The module declares Go 1.24 or later; local verification used Go 1.27.1 and PostgreSQL 17. The Docker build also uses Go 1.27.1. The only directly imported external Go dependency is the pinned [pgx driver and pool](https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool).

```sh
go mod download
go vet ./...
go test -race ./...

# Enable real PostgreSQL integration tests against the local demo instance.
TEST_DATABASE_URL='postgres://workq:local-demo-only@127.0.0.1:15433/workq?sslmode=disable' \
  go test -race -count=1 ./...
```

Without `TEST_DATABASE_URL`, unit tests run and integration tests explicitly skip. Each integration test creates its own randomly named `workq_test_*` schema and drops only that schema afterward; it never truncates the application's tables. CI provisions its own database and runs vet, the race detector, integration tests, and the image build. The badge above links to the workflow and its recorded results.

Tests cover concurrent idempotent submission, 10 simultaneous consumers processing 40 distinct jobs, retry invisibility before its schedule, dead-letter attempt caps, expired-lease recovery, rejection of stale acknowledgments, replay after a crash following the durable effect, heartbeats during jobs longer than a lease, API input and auth behavior, and handler panic/timeout/shutdown handling. These are correctness scenarios, not a throughput benchmark. Run `python3 scripts/smoke.py` against a running local stack for an HTTP-level demo check.

To run a binary outside Docker:

```sh
go build -o bin/workq ./cmd/workq
export DATABASE_URL='postgres://workq:local-demo-only@127.0.0.1:15433/workq?sslmode=disable'
bin/workq api
# In another terminal with DATABASE_URL set:
bin/workq worker
```

## Configuration and deployment review

| Environment variable | Default | Role |
| --- | --- | --- |
| `DATABASE_URL` | Required | PostgreSQL connection; use TLS for remote databases |
| `HTTP_ADDR` | `127.0.0.1:8080` | API bind address; container uses `:8080` |
| `API_TOKEN` | Empty | Shared Bearer token for all `/v1/*` routes |
| `WORKER_CONCURRENCY` | `2` | Goroutines per process; accepted range 1–32 |
| `LEASE_DURATION` | `5s` | Minimum 300 ms |
| `POLL_INTERVAL` | `250ms` | Polling interval when no ready job is found |
| `RETRY_BASE` / `RETRY_MAX` | `500ms` / `30s` | Handler-failure retry caps |
| `JOB_TIMEOUT` | `30s` | Handler context deadline |

Set `WORKQ_API_TOKEN` when starting Compose to pass `API_TOKEN` into the API. Include `Authorization: Bearer …` in curl and use the dashboard token field. The dashboard keeps that token in memory only. Health endpoints and static dashboard remain public; job content and state routes require the token when configured.

For public hosting, use a TLS reverse proxy with an ingress request rate limit, set a strong shared token, remove the host PostgreSQL port, replace demo credentials, and configure backups. Shared-token auth is appropriate for a private portfolio demo; this is not a multi-tenant identity system.

Known limits: one PostgreSQL primary, polling rather than push notifications, no priority or cancellation, no automatic dead-letter replay, no storage retention policy, no database failover controller, and no load benchmark. Bootstrap migrations are idempotent for this initial schema; later schema changes need versioned migrations. A future handler must honor context cancellation and implement its own effect idempotency. Read [DECISIONS.md](DECISIONS.md) for tradeoffs and [LEARNING.md](LEARNING.md) for the code walkthrough.
