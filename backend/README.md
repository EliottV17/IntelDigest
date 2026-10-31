# IntelDigest Python package tooling

This directory contains an **experimental, installable Python backend package** with passive configuration and an async job repository. It has no API, worker, or scraper entrypoint; the Go backend remains operational. SQL migrations remain in the repository-root `migrations/` directory.

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
| FastAPI | Future HTTP contract and request/response validation surface; no route is implemented here. |
| Pydantic | Explicit data validation dependency for the planned HTTP contract. |
| Uvicorn | ASGI server, without its standard extras or dotenv support. |
| asyncpg | Async PostgreSQL driver; SQL remains explicit and no ORM is added. |
| redis | Redis client, including `redis.asyncio`, for the existing Streams protocol. |
| aiohttp | Async HTTP client for the future bounded public-page fetcher. |
| readability-lxml, lxml | Article extraction and its native HTML/XML parser dependency. |
| pytest | Focused package test runner. |
| pytest-asyncio | Async test support for the planned async boundaries. |
| HTTPX | In-process HTTP client for future FastAPI tests. |
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

`inteldigest.config` provides passive, separate `load_api(env=None)` and `load_worker(env=None)` functions. Pass an explicit mapping for deterministic tests; omitted `env` reads `os.environ`. They only validate settings: they do not load dotenv, resolve DNS, connect to PostgreSQL/Redis, log values, or start processes. The Go backend remains operational; these Python loaders are not wired to an API or worker.

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
- `inteldigest.jobs.models.Job` retains UUID, timezone-aware database timestamps, status, optional error and decoded JSONB values. Pydantic's `model_dump(mode="json")` / `model_dump_json()` serialize UUIDs, timestamps and nested JSON natively, including null fields. HTTP omission rules remain M06's responsibility.

Claims and scrape diagnostics (M05), HTTP routes (M06), and POST/publication are not implemented.

### Repository tests

With the locked virtualenv already installed, no dependency resolution or service access is needed for unit tests and lint:

```sh
cd backend
.venv/bin/python -m pytest tests/unit/jobs/test_repository.py -q
.venv/bin/python -m pytest -q
.venv/bin/ruff check src tests
```

The default suite skips two PostgreSQL integration cases. They exercise creation/lookup, missing jobs, defaults, JSONB, diagnostics and timestamp round trips using the existing migration. **Running them requires separate operator authorization and an already-created, dedicated local `inteldigest_test` database.** They never create/drop a database or start services; each test rolls back its transaction, including any table creation by the migration. Do not run alongside other integration suites or on shared instances. No dotenv or environment DSN is read.

After that authorization, an operator may run serially with an explicitly supplied test URL (example only):

```sh
.venv/bin/python -m pytest tests/integration/test_repository.py -q \
  --test-database-url 'postgresql://localhost/inteldigest_test'
```

The option rejects non-local hosts, other database names, query parameters and fragments. Real PostgreSQL execution has not yet been verified for M04; fake-based unit checks do not establish driver/SQL integration.

## Package layout

The approved migration layout is recorded in [PLAN-MIGRATION.md](../PLAN-MIGRATION.md#31-estructura-de-paquetes-aprobada). Only the modules needed by completed/current tasks exist:

```text
src/inteldigest/
  __init__.py
  config.py
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
  integration/
    conftest.py
    test_repository.py
```

`config.py` stays transversal; job models and SQL live together in `jobs/`. HTTP schemas will belong to the future `api/` package, not to the persisted job model. API, queue, scraper and worker packages are not created ahead of their tasks.

Unit tests require no services. PostgreSQL fixtures live only in `tests/integration/conftest.py`; the root `conftest.py` merely registers the explicit connection option so pytest can recognize it consistently. Pytest uses `--import-mode=importlib` to collect identically named unit/integration test modules without package-name collisions or modifying `sys.path`.

## Package status and boundary

The package uses `src/` layout and has an inert `inteldigest` package initializer. Its smoke test checks installed metadata and imports the selected runtime dependencies. Configuration and repository methods are available but not wired to processes. Do not use Uvicorn startup commands: no ASGI application exists.

Generated virtualenv, uv cache, build output, pytest cache, and Python bytecode are local artifacts and ignored by Git. The existing Go implementation and its commands remain the operational reference until a separately authorized migration task changes that status.
