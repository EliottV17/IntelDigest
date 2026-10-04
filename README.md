# IntelDigest

IntelDigest accepts technical-article URLs, persists digest jobs, and publishes them for later processing. A manually launched worker can now consume new Redis Stream entries and scrape public HTML. Its synchronous result receiver is metadata-only and nondurable: jobs remain `processing` and entries stay unacknowledged until a later persistence/consolidation stage exists.

## Architecture and current status

```
Client ──POST /api/v1/digests──▸ API ──persist job──▸ PostgreSQL
                                  │                       ▲
                                  └──publish──▸ Redis Streams ──consume──▸ Worker ──fetch──▸ Public HTML
                                                                          │
                                                                          └──metadata-only, in-memory handoff
```

| Component | Technology | Status |
|---|---|---|
| API | Go, `net/http` | Accepts URLs, persists jobs, publishes to Redis, exposes job status |
| PostgreSQL | 17-alpine | Persists jobs and results (JSONB) |
| Redis | 7-alpine Streams | Receives API-published job messages; the worker creates/uses a shared consumer group |
| Worker and scraper | Go, Redis Streams, bounded HTTP | CLI wiring and fake-tested startup are implemented; live end-to-end operation is not verified |
| OpenRouter and frontend | — | Not implemented |

The Stream entry's `data` field contains JSON with `schema_version`, `job_id`, and `url`. PostgreSQL remains the source of truth for job state.

## Requirements

- Go 1.22+
- Docker and Docker Compose
- `psql` for manual migrations and PostgreSQL integration tests
- `curl` and `jq` for endpoint checks

## Run locally

Start the declared PostgreSQL and Redis services:

```bash
docker compose up -d postgres redis
```

Apply the database migration:

```bash
psql "postgres://inteldigest:inteldigest@localhost:5432/inteldigest?sslmode=disable" \
  -f migrations/001_create_jobs.up.sql
```

Start the API with the required database and Redis URLs. The Redis URL below is a safe local example without embedded credentials:

```bash
DATABASE_URL="postgres://inteldigest:inteldigest@localhost:5432/inteldigest?sslmode=disable" \
REDIS_URL="redis://localhost:6379/0" \
  go run ./cmd/api
```

`REDIS_URL` is required. API startup validates Redis configuration and connectivity; an unavailable or invalid Redis connection prevents startup. The server listens on `:8080` by default, configurable with `API_PORT`.

### Run the worker manually

After starting PostgreSQL and Redis, applying the migration, and providing the worker's required `DATABASE_URL` and `REDIS_URL`, a human can run the foreground worker with:

```bash
go run ./cmd/worker
```

This is an operator command, not an agent-executed step or a claim of live-service verification. The worker registers SIGINT/SIGTERM before loading configuration or opening dependencies, ensures its consumer group before reading, and closes Redis then PostgreSQL after the pool has joined. It creates no terminal job state: successful and failed scrape outputs are handed synchronously to a metadata-only receiver, remain in memory only, and are not ACKed or recovered after restart. Do not treat this incremental worker as a durable or production-ready processing pipeline.

### Queue environment variables

| Variable | Required | Default | Description |
|---|---:|---|---|
| `REDIS_URL` | Yes | — | Redis connection URL, for example `redis://localhost:6379/0` locally |
| `REDIS_STREAM` | No | `inteldigest:jobs` | Redis Stream to which the API publishes jobs |
| `REDIS_DIAL_TIMEOUT` | No | `5s` | Initial Redis connection timeout |
| `REDIS_PUBLISH_TIMEOUT` | No | `2s` | Per-publication timeout |

Worker-only settings are validated by the worker loader and do not alter API startup. All durations use Go duration syntax; invalid or out-of-range values fail worker startup.

| Variable | Default | Bounds / meaning |
|---|---:|---|
| `WORKER_POOL_SIZE` | `4` | `1`–`64` persistent workers; no in-memory job queue |
| `WORKER_CONSUMER_GROUP` | `inteldigest:workers` | Nonblank group shared by instances |
| `WORKER_CONSUMER_NAME` | Generated hostname plus UUID | If set, must be nonblank and unique among live processes |
| `WORKER_STREAM_BLOCK` | `2s` | At least `1ms`; finite Redis read block, not zero/infinite |
| `WORKER_QUEUE_OPERATION_TIMEOUT` | `5s` | Positive group/read/ack operation budget; read also includes its block budget |
| `WORKER_DB_TIMEOUT` | `5s` | Positive per-database-operation budget |
| `WORKER_SHUTDOWN_TIMEOUT` | `10s` | Positive processing drain window; not a hard process-exit deadline |
| `SCRAPER_CONNECT_TIMEOUT` | `5s` | Positive DNS/connect budget |
| `SCRAPER_TLS_TIMEOUT` | `5s` | Positive TLS handshake budget |
| `SCRAPER_HEADER_TIMEOUT` | `10s` | Positive response-header budget |
| `SCRAPER_READ_TIMEOUT` | `10s` | Positive total response-body read budget |
| `SCRAPER_REQUEST_TIMEOUT` | `30s` | Positive global request deadline shared across redirects |
| `SCRAPER_MAX_BODY_BYTES` | `5242880` | Positive decompressed body limit; must leave room for a safe max+1 read |
| `SCRAPER_MAX_REDIRECTS` | `5` | `0`–`10`; zero disables redirects |

The worker reuses `DATABASE_URL`, `REDIS_URL`, `REDIS_STREAM` (default `inteldigest:jobs`), and `REDIS_DIAL_TIMEOUT` (default `5s`). An unset consumer name is generated; it is not an empty configured name.

### Worker processing and safety boundaries

The worker reads only new entries from its shared Redis consumer group. Its receiver logs limited technical metadata; it does not persist or print article text, create another stream, call AI, or mark jobs complete. Both successful and failed scrape outputs remain nondurable in-memory handoffs; jobs stay `processing`, entries remain unacknowledged, and there is no business retry, pending-entry recovery, or automatic restart recovery. A process restart loses the handoff and does not rescue stuck work.

Scraping is restricted to public HTTP(S) destinations. DNS results are checked on each connection and the transport dials only a validated public IP literal while retaining the original hostname for the HTTP Host header and normal TLS certificate/SNI verification. Proxies and unsafe TLS overrides are disabled. Redirects are revalidated; the request deadline is shared across hops, the decompressed response body is bounded, and the configured pool limits concurrent jobs without an accumulating memory queue.

Shutdown first stops admission, then gives active jobs the configured drain window before canceling them, while waiting for bounded blocked reads and all workers before closing clients. If a finite read budget exceeds the drain window, process exit can take longer than that window: it is not a hard exit deadline.

`DATABASE_URL` is also required. `API_PORT` defaults to `8080` for the API. The worker does not use `API_PORT`.

## API behavior

`POST /api/v1/digests` persists a job, then publishes it to Redis before returning `202 Accepted`. On success, the response includes the `job_id`; use `GET /api/v1/digests/{id}` to inspect the persisted job. Unless an operator separately starts the worker, a successfully queued job remains `pending`. With the worker running, a claimed job remains `processing`; neither successful scraping nor a scrape error is a terminal status.

If Redis publication fails after persistence, the API responds with `503 Service Unavailable`, including the created job ID, for example:

```json
{"error":"queue unavailable","job_id":"<uuid>"}
```

The job remains persisted; the API does not pretend the database write was rolled back. There is no transactional outbox or POST idempotency key. In particular, Redis may have accepted `XADD` even if its acknowledgement was lost; the API can then return 503 although the entry exists. Retrying the POST can create a duplicate job.

## Try the API

```bash
# Health check
curl -s http://localhost:8080/healthz | jq .

# Create a digest job (publishes it to Redis; a manually started worker may consume it)
curl -s -X POST http://localhost:8080/api/v1/digests \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://go.dev/blog/go1.22"}' | jq .

# Inspect persisted job state
curl -s http://localhost:8080/api/v1/digests/<uuid> | jq .
```

## Tests

Unit tests do not require Redis:

```bash
go test ./...
```

Integration tests require already-running local PostgreSQL and Redis services; the test target does not start, stop, migrate, or reset application services. If needed, a human can start only the declared services with `docker compose up -d postgres redis`. The tests use explicit local-only connection URLs and may be run serially with:

```bash
make test-integration
```

The target runs the database, queue, and worker integration packages with `-p 1` because their isolated database lifecycle must not overlap. PostgreSQL tests derive and mutate only `inteldigest_test`, guarded by the test harness; they never mutate the application `inteldigest` database. Queue and worker tests create unique Redis stream/group names and delete only their owned stream during cleanup. Services must be ready before running the command.

Worker integration coverage exercises the real PostgreSQL repository, Redis consumer, processor, and pool together, but injects a deterministic fake `ArticleScraper`. It verifies atomic duplicate claims, safe scrape-error diagnosis, unacknowledged processing failures, terminal duplicate ACKs, and restart behavior that leaves old pending work unrecovered. This is not a production CLI/live end-to-end run and does not prove the real HTTP article pipeline; scraper HTTP/TLS/public-destination policy is covered separately by scraper tests. The synchronous result receiver is still nondurable, so successful and failed jobs remain `processing` and are not ACKed.

## Repository layout

```
cmd/
  api/            # HTTP server and dependency wiring
  worker/         # Foreground queue consumer CLI
internal/
  api/            # HTTP handlers and routing
  config/         # Environment configuration
  db/             # PostgreSQL repository
  models/         # Job model and URL validation
  queue/          # Redis Streams publisher and message contract
  scraper/        # Public-URL policy, bounded fetching, and article extraction
migrations/       # Versioned SQL migrations
```
