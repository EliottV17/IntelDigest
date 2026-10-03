# IntelDigest

IntelDigest accepts technical-article URLs, persists digest jobs, and publishes them for later processing. The API currently publishes jobs to Redis Streams, but **there is no worker yet**: jobs remain `pending` and are not scraped or processed.

## Architecture and current status

```
Client ──POST /api/v1/digests──▸ API ──persist job──▸ PostgreSQL
                                  │
                                  └──publish──▸ Redis Streams
                                                (no consumer yet)
```

| Component | Technology | Status |
|---|---|---|
| API | Go, `net/http` | Accepts URLs, persists jobs, publishes to Redis, exposes job status |
| PostgreSQL | 17-alpine | Persists jobs and results (JSONB) |
| Redis | 7-alpine Streams | Receives API-published job messages; no worker consumes them yet |
| Worker, scraper, OpenRouter, frontend | — | Not implemented |

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

### Queue environment variables

| Variable | Required | Default | Description |
|---|---:|---|---|
| `REDIS_URL` | Yes | — | Redis connection URL, for example `redis://localhost:6379/0` locally |
| `REDIS_STREAM` | No | `inteldigest:jobs` | Redis Stream to which the API publishes jobs |
| `REDIS_DIAL_TIMEOUT` | No | `5s` | Initial Redis connection timeout |
| `REDIS_PUBLISH_TIMEOUT` | No | `2s` | Per-publication timeout |

`DATABASE_URL` is also required. `API_PORT` defaults to `8080`.

## API behavior

`POST /api/v1/digests` persists a job, then publishes it to Redis before returning `202 Accepted`. On success, the response includes the `job_id`; use `GET /api/v1/digests/{id}` to inspect the persisted job. Since no worker exists, a successfully queued job remains `pending`.

If Redis publication fails after persistence, the API responds with `503 Service Unavailable`, including the created job ID, for example:

```json
{"error":"queue unavailable","job_id":"<uuid>"}
```

The job remains persisted; the API does not pretend the database write was rolled back. There is no transactional outbox or POST idempotency key. In particular, Redis may have accepted `XADD` even if its acknowledgement was lost; the API can then return 503 although the entry exists. Retrying the POST can create a duplicate job.

## Try the API

```bash
# Health check
curl -s http://localhost:8080/healthz | jq .

# Create a digest job (publishes it to Redis; no worker processes it yet)
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

Redis publication integration coverage is opt-in and requires a reachable Redis instance. For the Compose service, start it with `docker compose up -d redis`, then run:

```bash
REDIS_URL=redis://localhost:6379/0 go test -tags integration ./internal/queue
```

The integration test uses a unique Stream key, verifies the published entry's exact JSON `data` payload, and deletes its Stream on cleanup. PostgreSQL integration tests require PostgreSQL:

```bash
DATABASE_URL="postgres://inteldigest:inteldigest@localhost:5432/inteldigest?sslmode=disable" \
  go test -tags integration ./internal/db/...
```

## Repository layout

```
cmd/
  api/            # HTTP server and dependency wiring
  worker/         # Future queue consumer (not implemented)
internal/
  api/            # HTTP handlers and routing
  config/         # Environment configuration
  db/             # PostgreSQL repository
  models/         # Job model and URL validation
  queue/          # Redis Streams publisher and message contract
  scraper/        # Future content extraction (not implemented)
migrations/       # Versioned SQL migrations
```
