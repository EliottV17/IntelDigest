# IntelDigest Python package tooling

This directory contains an **experimental, installable Python backend package** with configuration, an async job repository and a read-only FastAPI application. POST, Redis transport, worker and scraper are not implemented in Python; the Go backend remains operational. SQL migrations remain in the repository-root `migrations/` directory.

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
| FastAPI | Read-only HTTP routes, lifespan and response validation. |
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

## Job creation and lookup (M04)

`inteldigest.jobs.repository.Repository` borrows an asyncpg pool or connection with default codecs. It does not open/close connections or run migrations. The caller owns connection lifecycle, operation timeouts and transaction policy; cancellation and database errors propagate without retries or logging. URL validation belongs to the future submission endpoint, not the repository.

- `await repository.create_job(url)` binds a fresh UUID and the URL in an `INSERT ... RETURNING`. The existing SQL defaults supply `pending`, equal creation/update timestamps, and null result/error.
- `await repository.get_job_by_id(uuid)` returns the stored job or raises `JobNotFound("job not found")`. Other database errors are not treated as missing jobs.
- `inteldigest.jobs.models.Job` retains UUID, timezone-aware database timestamps, status, optional error and decoded JSONB values. Pydantic's `model_dump(mode="json")` / `model_dump_json()` serialize UUIDs, timestamps and nested JSON natively, including null fields. The HTTP layer omits absent top-level `result`/`error` fields without changing the storage model.

## Atomic claims and scrape diagnostics (M05)

The same repository also exposes two conditional writes:

- `await repository.try_mark_processing(uuid)` performs one atomic `UPDATE ... WHERE status = 'pending' RETURNING id`, setting `processing`, clearing any previous error and updating `updated_at`. Concurrent callers can have only one winner. It returns `True` for the winner and `False` for missing/already-claimed/terminal jobs.
- `await repository.record_scrape_error(uuid, safe_reason)` updates only `error` and `updated_at` on a job still in `processing`. It retains at most 1024 Unicode code points (not bytes or grapheme clusters) using ordinary Python slicing, including empty diagnostics. It returns `False` for missing jobs or any other state, without modifying them.

With PostgreSQL's default `READ COMMITTED` isolation, a competing claim rechecks `pending` after the winner commits and returns `False`. Stronger isolation levels can instead raise a serialization error; that error propagates without retries. If the caller supplies a transaction, successful updates are provisional until it commits.

Both methods preserve `result`, URL, ID and creation time. Diagnostics do not mark jobs `failed` or `completed`, publish messages or ACK Redis entries. Database errors and cancellation propagate; there are no implicit retries. The caller must provide an already-safe reason: bounding a string is not redaction of secrets or article content.

The read-only HTTP routes below use lookup only. POST/publication remains deferred to B2.

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

## Read-only HTTP API (M06)

With the locked virtualenv installed, PostgreSQL already running, the root SQL migration already applied by an operator, and configuration exported by the human:

```sh
cd backend
env -u UVICORN_ENV_FILE .venv/bin/python -m uvicorn inteldigest.api.app:create_app \
  --factory --host 0.0.0.0 --port "${API_PORT:-8080}"
```

This is a manual service command, not a compilation check. No dotenv file is loaded; do not add `--env-file`. The port is passed explicitly from `API_PORT` (default `8080`). The existing API loader still requires `DATABASE_URL` and a valid `REDIS_URL`, but M06 does not connect to Redis. Redis connectivity/publication belongs to B2. Malformed worker-only options do not affect API loading.

The factory is passive. Lifespan validates API configuration, opens one asyncpg pool (minimum 1, maximum 10 connections, 5-second connection timeout, 10-second query timeout) and makes the existing repository available to requests. Startup failure prevents serving. Shutdown closes the pool with a 10-second close budget; asyncpg terminates connections if that close is cancelled. No migration or data write happens at startup.

| Request | Response |
|---|---|
| `GET /healthz` | `200 {"status":"ok"}`; liveness, not a database/Redis readiness query |
| `GET /api/v1/digests/{id}` | `200` with stored job fields |
| Invalid UUID | `400 {"error":"invalid job ID"}`, without a database lookup |
| Missing job | `404 {"error":"job not found"}` |
| Lookup failure | `500 {"error":"internal error"}`; raw database exceptions are not logged or returned |

Both read routes also support `HEAD` with the same status/headers and no response body. IDs are serialized as canonical lowercase hyphenated UUIDs. Python's native UUID parser also accepts compact, braced and UUID URN input; no Go parser emulation is added. Timestamps use Pydantic's timezone-aware ISO 8601/RFC 3339 serialization, preserving the instant and microseconds (`Z` for UTC). Absent `result` and `error` fields are omitted, not emitted as JSON null; a present empty error, JSON false/zero/empty value and nested JSON null are preserved.

Example after manual startup, using an ID already in the database:

```sh
curl -s http://localhost:8080/healthz
curl -s 'http://localhost:8080/api/v1/digests/<uuid>'
```

M06 tests use TestClient's lifespan and a deterministic pool double, including the real repository's row conversion. They prove HTTP behavior and resource ownership without services; they are not a live HTTP + PostgreSQL run. Run `.venv/bin/python -m pytest tests/unit/api/test_app.py -q`. The installed Starlette emits an HTTPX deprecation warning; tests still pass and no unapproved dependency change is made.

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
tests/
  conftest.py
  unit/
    test_config.py
    test_package.py
    jobs/test_repository.py
    api/test_app.py
  integration/
    conftest.py
    test_repository.py
```

`config.py` stays transversal; job models and SQL live together in `jobs/`. HTTP schemas live in `api/`, separate from the persisted job model. Queue, scraper and worker packages are not created ahead of their tasks.

Unit tests require no services. PostgreSQL fixtures live only in `tests/integration/conftest.py`; the root `conftest.py` merely registers the explicit connection option so pytest can recognize it consistently. Pytest uses `--import-mode=importlib` to collect identically named unit/integration test modules without package-name collisions or modifying `sys.path`.

## Package status and boundary

The package uses `src/` layout and has an inert `inteldigest` package initializer. Its smoke test checks installed metadata and imports the selected runtime dependencies. The read-only API is wired to configuration and repository through its lifespan. It does not create jobs, publish/consume messages or process articles.

Generated virtualenv, uv cache, build output, pytest cache, and Python bytecode are local artifacts and ignored by Git. The existing Go implementation and its commands remain the operational reference until a separately authorized migration task changes that status.
