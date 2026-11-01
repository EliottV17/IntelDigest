# IntelDigest Python package tooling

This directory contains an **experimental, installable Python backend package** with configuration, an async job repository, initial non-DNS URL validation, a FastAPI application with health, job lookup and submission endpoints, the Redis Stream v1 job publication contract, and bounded Redis Stream consumption. Worker execution and scraper are not implemented in Python yet; the Go backend remains operational. SQL migrations remain in the repository-root `migrations/` directory.

## Quick path

Use `uv` 0.13.1 and Python 3.14.8. The project pins Python in `.python-version` and requires Python `>=3.14,<3.15`.

```sh
cd backend
UV_CACHE_DIR="$PWD/.uv-cache" UV_PYTHON_DOWNLOADS=never UV_NO_ENV_FILE=1 uv sync --frozen
UV_CACHE_DIR="$PWD/.uv-cache" UV_PYTHON_DOWNLOADS=never UV_NO_ENV_FILE=1 uv run --no-env-file --frozen python -m pytest
UV_CACHE_DIR="$PWD/.uv-cache" UV_PYTHON_DOWNLOADS=never UV_NO_ENV_FILE=1 uv build --no-build-isolation
```

The first command creates the project-local `.venv` from the committed lock. `UV_PYTHON_DOWNLOADS=never` prevents uv from downloading an interpreter. `UV_NO_ENV_FILE=1` and `--no-env-file` ensure uv does not load dotenv files. `uv build --no-build-isolation` writes wheel and source archive artifacts into `dist/` using the locked Hatchling in the synced environment. Because the required uv cache is inside this source tree, uv emits a cache-location warning; build exclusions keep that cache, the virtualenv, and generated artifacts out of distributions.

To check lock freshness without changing the lock:

```sh
UV_CACHE_DIR="$PWD/.uv-cache" UV_PYTHON_DOWNLOADS=never UV_NO_ENV_FILE=1 uv lock --check
```

## Dependencies

| Direct dependency | Purpose |
|---|---|
| FastAPI | HTTP routes, lifespan, submission/lookup and response validation. |
| Pydantic | Job data and HTTP response validation/serialization. |
| Uvicorn | ASGI server, without its standard extras or dotenv support. |
| asyncpg | Async PostgreSQL driver; SQL remains explicit and no ORM is added. |
| redis | Redis client, including `redis.asyncio`, for the existing Streams protocol. |
| aiohttp | Async HTTP client for the future bounded public-page fetcher. |
| readability-lxml, lxml | Article extraction and its native HTML/XML parser dependency. |
| pytest | Focused package test runner. |
| pytest-asyncio | Async test support for the planned async boundaries. |
| HTTPX | In-process HTTP contract tests via FastAPI's TestClient. |
| hatchling | Minimal PEP 517 package build backend, also included in the locked development tools so builds need no untracked isolated build resolution. |
| Ruff | Development-only Python linter; it does not replace Pyright's type checking. |

`uv.lock` records exact transitive and direct runtime/development resolutions for Python 3.14.8. Runtime dependencies deliberately exclude FastAPI/Uvicorn standard extras and dotenv loaders. The Python requirement is intentionally limited to the installed, locally validated 3.14 series at this migration stage.

Build-isolation boundary: the first `uv sync --frozen` builds the editable project in isolation and may resolve `build-system.requires` separately; those isolated build dependencies are not locked by `uv.lock`. The documented `uv build --no-build-isolation` instead uses Hatchling from the synchronized, locked development group. Offline reconstruction was independently verified using an existing artifact cache, not a cache-empty machine. These checks establish package installation and artifact usability, not byte-for-byte reproducible builds.

## Python linting

Ruff is a development-only linter for source and tests; it does not replace Pyright, which checks types and uses the project-local virtualenv configured in `pyrightconfig.json`. Ruff's default rules are used without a project-specific ruleset.

Run Ruff from `backend/` with the same explicit uv environment safeguards used above:

```sh
UV_CACHE_DIR="$PWD/.uv-cache" UV_PYTHON_DOWNLOADS=never UV_NO_ENV_FILE=1 uv run --no-env-file --frozen ruff check src tests
UV_CACHE_DIR="$PWD/.uv-cache" UV_PYTHON_DOWNLOADS=never UV_NO_ENV_FILE=1 uv run --no-env-file --frozen ruff check --fix src tests
```

The first command reports lint findings without changing files. The second applies Ruff's safe automatic fixes; review the resulting diff. Ruff formatting is not adopted as part of this workflow.

## Configuration loaders (M03)

`inteldigest.config` provides passive, separate `load_api(env=None)` and `load_worker(env=None)` functions. Pass an explicit mapping for deterministic tests; omitted `env` reads `os.environ`. They only validate settings: they do not load dotenv, resolve DNS, connect to PostgreSQL/Redis, log values, or start processes. The API loader is used by the FastAPI lifespan; the worker loader is not wired to a process yet.

Quick use (no services are contacted):

```python
from inteldigest.config import load_api, load_worker

api = load_api()       # reads API/shared settings from the process environment
worker = load_worker() # reads shared and worker settings; API-only settings are ignored
print(api.api_port, worker.pool_size)
```

Duration fields end in `_seconds` and contain Python `float` seconds. Environment values are numeric seconds such as `5` or `0.5`; unit strings such as `500ms` are not accepted. Timeouts must be finite and positive, except `WORKER_STREAM_BLOCK`, which must be at least `0.001` seconds so Redis blocking remains finite. `queue_read_timeout_seconds` is the block plus operation timeout. Numeric conversion follows Python's native `float()` behavior rather than Go duration syntax.

| Variable | Used by | Default | Validation / meaning |
|---|---|---:|---|
| `DATABASE_URL` | API, worker | Required | Nonempty string; retained verbatim. |
| `REDIS_URL` | API, worker | Required | `redis://` or `rediss://` URL with host; path is empty, `/`, or a non-negative integer database. Credentials are never included in validation errors. |
| `REDIS_STREAM` | API, worker | `inteldigest:jobs` | Empty or unset uses default; nonempty value retained verbatim. |
| `REDIS_DIAL_TIMEOUT` | API, worker | `5` | Finite positive number of seconds (for example `0.5`). |
| `API_PORT` | API only | `8080` | Empty or unset uses default; otherwise retained verbatim. |
| `REDIS_PUBLISH_TIMEOUT` | API only | `2` | Finite positive number of seconds. Worker ignores it. |
| `WORKER_POOL_SIZE` | Worker only | `4` | Integer `1`–`64`. |
| `WORKER_CONSUMER_GROUP` | Worker only | `inteldigest:workers` | If set, must be nonblank; surrounding whitespace is trimmed. |
| `WORKER_CONSUMER_NAME` | Worker only | Hostname plus UUID | If set, must be nonblank; surrounding whitespace is trimmed. Unset names are generated uniquely per loader call. |
| `WORKER_STREAM_BLOCK` | Worker only | `2` | Numeric seconds, minimum `0.001` to prevent infinite Redis blocking. |
| `WORKER_QUEUE_OPERATION_TIMEOUT` | Worker only | `5` | Finite positive number of seconds. |
| `WORKER_DB_TIMEOUT` | Worker only | `5` | Finite positive number of seconds. |
| `WORKER_SHUTDOWN_TIMEOUT` | Worker only | `10` | Finite positive seconds; not a hard process-exit deadline. |
| `SCRAPER_CONNECT_TIMEOUT` | Worker only | `5` | Finite positive number of seconds. |
| `SCRAPER_TLS_TIMEOUT` | Worker only | `5` | Finite positive number of seconds. |
| `SCRAPER_HEADER_TIMEOUT` | Worker only | `10` | Finite positive number of seconds. |
| `SCRAPER_READ_TIMEOUT` | Worker only | `10` | Finite positive number of seconds. |
| `SCRAPER_REQUEST_TIMEOUT` | Worker only | `30` | Finite positive seconds; one global deadline, not summed across redirect stages. |
| `SCRAPER_MAX_BODY_BYTES` | Worker only | `5242880` | Positive integer; the read limit is one byte higher. |
| `SCRAPER_MAX_REDIRECTS` | Worker only | `5` | Integer `0`–`10`; zero disables redirects. |

API loading intentionally ignores malformed worker-only settings. Worker loading ignores `API_PORT` and `REDIS_PUBLISH_TIMEOUT`, and does not require them.

## Job creation, validation, and lookup (M04, M08)

`inteldigest.jobs.validation.validate_url` performs initial non-DNS HTTP(S) URL validation before persistence. It rejects empty/whitespace strings, control characters, internal or surrounding whitespace, malformed URL syntax/ports, missing hosts, `localhost` (including trailing-dot `localhost.`), and loopback/private/link-local/multicast/unspecified/reserved IP literals (including IPv4-mapped IPv6). Public IP literals and domain names pass without performing any DNS lookup.

`inteldigest.jobs.repository.Repository` borrows an asyncpg pool or connection with default codecs. It does not open/close connections or run migrations. The caller owns connection lifecycle, operation timeouts and transaction policy; cancellation and database errors propagate without retries or logging. URL validation belongs to the submission endpoint via `validate_url`, not the repository.

- `await repository.create_job(url)` binds a fresh UUID and the URL in an `INSERT ... RETURNING`. The existing SQL defaults supply `pending`, equal creation/update timestamps, and null result/error.
- `await repository.get_job_by_id(uuid)` returns the stored job or raises `JobNotFound("job not found")`. Other database errors are not treated as missing jobs.
- `inteldigest.jobs.models.Job` retains UUID, timezone-aware database timestamps, status, optional error and decoded JSONB values. Pydantic's `model_dump(mode="json")` / `model_dump_json()` serialize UUIDs, timestamps and nested JSON natively, including null fields. The HTTP layer omits absent top-level `result`/`error` fields without changing the storage model.

## Atomic claims and scrape diagnostics (M05)

The same repository also exposes two conditional writes:

- `await repository.try_mark_processing(uuid)` performs one atomic `UPDATE ... WHERE status = 'pending' RETURNING id`, setting `processing`, clearing any previous error and updating `updated_at`. Concurrent callers can have only one winner. It returns `True` for the winner and `False` for missing/already-claimed/terminal jobs.
- `await repository.record_scrape_error(uuid, safe_reason)` updates only `error` and `updated_at` on a job still in `processing`. It retains at most 1024 Unicode code points (not bytes or grapheme clusters) using ordinary Python slicing, including empty diagnostics. It returns `False` for missing jobs or any other state, without modifying them.

With PostgreSQL's default `READ COMMITTED` isolation, a competing claim rechecks `pending` after the winner commits and returns `False`. Stronger isolation levels can instead raise a serialization error; that error propagates without retries. If the caller supplies a transaction, successful updates are provisional until it commits.

Both methods preserve `result`, URL, ID and creation time. Diagnostics do not mark jobs `failed` or `completed`, publish messages or ACK Redis entries. Database errors and cancellation propagate; there are no implicit retries. The caller must provide an already-safe reason: bounding a string is not redaction of secrets or article content.

The HTTP routes below use both lookup and submission. Consumer wiring and worker execution remain deferred to M09 and B4.

### Repository tests

With the locked virtualenv already installed, no dependency resolution or service access is needed for unit tests and lint:

```sh
cd backend
.venv/bin/python -m pytest tests/unit/jobs/test_repository.py -q
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests
```

The default suite skips twelve PostgreSQL integration cases. They exercise creation/lookup, missing jobs, defaults, JSONB/timestamps, conditional writes in all four states, and a two-connection claim with exactly one winner. **Running them requires separate operator authorization and an already-created, dedicated local `inteldigest_test` database.** They never create/drop a database or start services. Most tests roll back their transactions, including any table creation by the existing migration. The contention test creates a uniquely named `m05_<uuid>` schema, applies that same migration inside it, and uses a two-connection pool; cleanup drops only its owned schema and test data. An interrupted process can leave that schema for manual cleanup. Do not run alongside other integration suites or on shared instances. No dotenv or environment DSN is read.

After that authorization, an operator may run serially with an explicitly supplied test URL (example only):

```sh
.venv/bin/python -m pytest tests/integration/test_repository.py -q \
  --test-database-url 'postgresql://localhost/inteldigest_test'
```

The option rejects non-local hosts, other database names, query parameters and fragments. The authorized M04/M05 integration run against the repository's local PostgreSQL 17 passed **all 12 cases**, including two real competing connections with exactly one claim winner. Schema names, public table names and job count matched their pre-test baseline afterward. This verifies the repository boundary only: it does not exercise an HTTP endpoint, Redis, the worker or scraping. The default suite still skips these cases unless the explicit test URL is provided; the observed run is not a guarantee that services are always available.

For that run, PostgreSQL alone was started with Compose using `COMPOSE_DISABLE_ENV_FILE=1`, `COMPOSE_ENV_FILES` removed and the explicit repository Compose file. The dedicated `inteldigest_test` database was created only because it was absent. No application database/volume was deleted, and the service/test database were left running/available. Startup and database creation remain operator-authorized steps, not implicit test behavior. Evidence: [M05 ledger](../odd/tasks/python-migration-m05.md#l9--observed-live-postgresql-integration).

## HTTP API (M06, M08)

With the locked virtualenv installed, PostgreSQL and Redis already running, the root SQL migration already applied by an operator, and configuration exported by the human:

```sh
cd backend
env -u UVICORN_ENV_FILE .venv/bin/python -m uvicorn inteldigest.api.app:create_app \
  --factory --host 0.0.0.0 --port "${API_PORT:-8080}"
```

This is a manual service command, not a compilation check. No dotenv file is loaded; do not add `--env-file`. The port is passed explicitly from `API_PORT` (default `8080`). The API loader requires `DATABASE_URL` and a valid `REDIS_URL`. Malformed worker-only options do not affect API loading.

The factory is passive. Lifespan validates API configuration, opens one asyncpg pool (minimum 1, maximum 10 connections, 5-second connection timeout, 10-second query timeout), connects the Redis Publisher with dial and publish timeouts, and provides the repository and publisher to requests. Startup failure (either database pool or Redis dial) prevents serving and closes any opened pool. On shutdown, both the Redis publisher and the database pool are closed with a 10-second close budget; an error in publisher close never skips closing the pool. No migration or data write happens at startup.

| Request | Response |
|---|---|
| `GET /healthz` | `200 {"status":"ok"}`; liveness, not a database/Redis readiness query |
| `GET /api/v1/digests/{id}` | `200` with stored job fields |
| Invalid UUID | `400 {"error":"invalid job ID"}`, without a database lookup |
| Missing job | `404 {"error":"job not found"}` |
| Lookup failure | `500 {"error":"internal error"}`; raw database exceptions are not logged or returned |
| `POST /api/v1/digests` valid URL | `202 {"job_id":"<uuid>","message":"processing"}`; persisted in DB before Redis publication |
| Malformed body / non-object JSON | `400 {"error":"invalid JSON body"}` without database or queue operations |
| Non-string `url` field | `400 {"error":"invalid JSON body"}` without database or queue operations |
| Invalid URL (empty, scheme, host, private/reserved, syntax) | `400 {"error":"<reason>"}` without database or queue operations |
| Repository creation failure | `500 {"error":"internal error"}` without calling Redis publisher |
| Redis publication failure | `503 {"error":"queue unavailable","job_id":"<uuid>"}`; persisted job is retained |

Both read routes also support `HEAD` with the same status/headers and no response body. IDs are serialized as canonical lowercase hyphenated UUIDs. Python's native UUID parser also accepts compact, braced and UUID URN input; no Go parser emulation is added. Timestamps use Pydantic's timezone-aware ISO 8601/RFC 3339 serialization, preserving the instant and microseconds (`Z` for UTC). Absent `result` and `error` fields are omitted, not emitted as JSON null; a present empty error, JSON false/zero/empty value and nested JSON null are preserved.

Example after manual startup:

```sh
curl -s http://localhost:8080/healthz
curl -s -X POST http://localhost:8080/api/v1/digests \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/article"}'
curl -s 'http://localhost:8080/api/v1/digests/<uuid>'
```

M06 and M08 tests use TestClient's lifespan and deterministic database pool and Redis publisher doubles. They prove HTTP behavior, submission validation, persist-before-publish ordering, error boundaries and resource ownership without services; they are not a live HTTP + PostgreSQL + Redis run. Run `.venv/bin/python -m pytest tests/unit/api/test_app.py -q`. The installed Starlette emits an HTTPX deprecation warning; tests still pass and no unapproved dependency change is made.

## Redis job publication (M07)

`inteldigest.queue` implements the versioned Redis Stream publication contract used by the API pipeline, without altering stream fields or adding consumer/worker logic yet:

- `inteldigest.queue.message.Message` (`SCHEMA_VERSION = 1`) is an immutable Pydantic model carrying `schema_version`, `job_id` and `url`. `model_dump_json()` serializes compact JSON `{"schema_version":1,"job_id":"<uuid>","url":"<url>"}`. Validation requires strict integer `schema_version == 1`, a non-nil UUID, and a syntactically valid absolute `http`/`https` URL with a host and no whitespace/control characters; it performs no DNS lookup.
- `await Publisher.connect(redis_url, stream, dial_timeout_seconds, publish_timeout_seconds)` validates the Redis URL (`redis://` or `rediss://` with host and optional non-negative DB index, without exposing credentials in errors), stream name, and finite positive timeouts before creating an async `redis.asyncio` client with `socket_connect_timeout=dial_timeout_seconds` and verifying connectivity via `PING` within `dial_timeout_seconds`. If `PING` fails, times out or is cancelled, the created client is closed before raising `PublisherConnectionError` (or propagating cancellation).
- `await publisher.publish(message)` validates the `Message` before calling Redis, then appends `{"data": payload_json}` via `XADD` to the configured stream under `publish_timeout_seconds`. Transport errors and timeouts raise `PublishError` naming the stream and preserving `__cause__`; external task cancellation propagates. `await publisher.aclose()` closes the underlying Redis client.

M07 is covered by deterministic unit tests (`.venv/bin/python -m pytest tests/unit/queue -q`) without starting or connecting to a live Redis instance. `Publisher` is wired into FastAPI lifespan and `POST /api/v1/digests` in M08.

## Bounded Redis Stream consumption (M09)

`inteldigest.queue.consumer` implements the consumer-side Redis Stream transport contract (`Consumer`, `Delivery`, and `ConsumerGroupMissingError`), without business state decisions or worker execution:

- `Delivery` (`entry_id: str`, `fields: Mapping[str, Any]`) is an immutable dataclass retaining the original stream entry identity and uninterpreted field mapping. `Consumer.read` is strictly transport-only: it does **not** decode or validate JSON payloads; payload decoding and job processing belong to the worker processor in M14.
- Normalization: `Consumer.read` decodes wire `bytes` entry IDs and field keys to UTF-8 strings. Field values in `bytes` are decoded using UTF-8 with replacement (`errors="replace"`), ensuring undecodable bytes never crash the reader while guaranteeing invalid payloads fail later JSON validation in the processor. Values already received as `str` or test-double primitives are preserved.
- `await Consumer.connect(redis_url, stream, group, consumer_name, dial_timeout_seconds=5.0, stream_block_seconds=2.0, operation_timeout_seconds=5.0)` validates configuration before connecting:
  - `redis_url`: `redis://` or `rediss://` with host and optional non-negative integer DB path; invalid URLs raise `ValueError("queue: invalid Redis URL")` with both `__cause__` and `__context__` set to `None` so credentials never leak.
  - `stream`: required nonempty string.
  - `group` and `consumer_name`: required nonblank strings with surrounding whitespace trimmed.
  - `stream_block_seconds`: finite float `>= 0.001` (at least 1ms); converted to integer milliseconds (`block_ms >= 1`) to prevent Redis `BLOCK 0` (which blocks indefinitely).
  - `dial_timeout_seconds` and `operation_timeout_seconds`: finite positive floats.
  - Verifies connectivity via `PING` within `dial_timeout_seconds`. If `PING` fails, times out, or is cancelled, the client is closed before raising `ConsumerConnectionError` (or re-raising `CancelledError`).
- `await consumer.ensure_group()` creates the group with `XGROUP CREATE stream group 0 MKSTREAM` under `operation_timeout_seconds`. Idempotency rule: only a Redis `ResponseError` starting with exact `"BUSYGROUP"` is treated as an already-existing group and returns cleanly (`None`). Any other error or driver timeout wraps into `QueueError` naming stream and group with `from exc`.
- `await consumer.read() -> Delivery | None` calls `XREADGROUP groupname consumername streams={stream: ">"} count=1 block=block_ms noack=False` under `stream_block_seconds + operation_timeout_seconds`. Normal empty block expirations (`[]`, `None`, or empty entry lists) return `None`. If Redis responds with `NOGROUP`, it raises `ConsumerGroupMissingError` (subclass of `QueueError`) naming stream and group with `from exc`, detectable via `is_group_missing(err)` or `isinstance`. Other driver errors or read timeouts wrap into `QueueError`. External `asyncio.CancelledError` propagates unmasked.
- `await consumer.ack(entry_id: str)` validates a nonblank string entry ID before calling Redis, then invokes `XACK stream group entry_id` under `operation_timeout_seconds`. It **never deletes stream entries** (`XDEL` and `DELETE` are never called). Errors wrap into `QueueError` naming stream, group, and entry ID; `CancelledError` propagates.
- `await consumer.aclose()` releases the owned Redis client and is idempotent.

Branch B2 (`migration/python-redis-api`) completes the Redis publication and consumption contracts (M07–M09) and the FastAPI submission endpoint (M08) with deterministic unit/HTTP doubles. Running live Redis or PostgreSQL+HTTP instances remains unexecuted. Scraper B3 and worker B4 remain pending in Python.

## Package layout

The approved migration layout is recorded in [PLAN-MIGRATION.md](../PLAN-MIGRATION.md#31-estructura-de-paquetes-aprobada). Only the modules needed by completed/current tasks exist:

```text
src/inteldigest/
  __init__.py
  config.py
  api/
    __init__.py
    app.py
    dependencies.py
    schemas.py
    routes/{__init__,health,digests}.py
  jobs/
    __init__.py
    models.py
    repository.py
    validation.py
  queue/
    __init__.py
    message.py
    publisher.py
    consumer.py
tests/
  conftest.py
  unit/
    test_config.py
    test_package.py
    jobs/{test_repository,test_validation}.py
    api/test_app.py
    queue/{test_message,test_publisher,test_consumer}.py
  integration/
    conftest.py
    test_repository.py
```

`config.py` stays transversal; job models, SQL and URL validation live together in `jobs/`. HTTP schemas live in `api/`, separate from the persisted job model. `queue/` holds the M07 message/publisher modules and M09 consumer module; scraper and worker modules are not created ahead of their tasks.

Unit tests require no services. PostgreSQL fixtures live only in `tests/integration/conftest.py`; the root `conftest.py` merely registers the explicit connection option so pytest can recognize it consistently. Pytest uses `--import-mode=importlib` to collect identically named unit/integration test modules without package-name collisions or modifying `sys.path`.

## Package status and boundary

The package uses `src/` layout and has an inert `inteldigest` package initializer. Its smoke test checks installed metadata and imports the selected runtime dependencies. The HTTP API is wired to configuration, database repository and Redis publisher through its lifespan; `inteldigest.queue` provides v1 message serialization, `XADD` publication, and bounded stream consumption, not yet wired to a standalone worker process or scraper.

Generated virtualenv, uv cache, build output, pytest cache, and Python bytecode are local artifacts and ignored by Git. The existing Go implementation and its commands remain the operational reference until a separately authorized migration task changes that status.
