# IntelDigest Python package tooling

This directory contains an **experimental, installable Python package only**. It does not run the API, worker, scraping, or database behavior; the Go backend remains the reference implementation. SQL migrations remain in the repository-root `migrations/` directory.

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

`uv.lock` records exact transitive and direct runtime/development resolutions for Python 3.14.8. Runtime dependencies deliberately exclude FastAPI/Uvicorn standard extras and dotenv loaders. The Python requirement is intentionally limited to the installed, locally validated 3.14 series at this migration stage.

Build-isolation boundary: the first `uv sync --frozen` builds the editable project in isolation and may resolve `build-system.requires` separately; those isolated build dependencies are not locked by `uv.lock`. The documented `uv build --no-build-isolation` instead uses Hatchling from the synchronized, locked development group. Offline reconstruction was independently verified using an existing artifact cache, not a cache-empty machine. These checks establish package installation and artifact usability, not byte-for-byte reproducible builds.

## Package status and boundary

The package uses `src/` layout and has an inert `inteldigest` package initializer. Its smoke test checks installed metadata and imports the selected runtime dependencies; it does not implement future behavior. There are no application entry points, settings loaders, API routes, worker loops, or service connections yet. Do not use Uvicorn startup commands: no ASGI application exists.

Generated virtualenv, uv cache, build output, pytest cache, and Python bytecode are local artifacts and ignored by Git. The existing Go implementation and its commands remain the operational reference until a separately authorized migration task changes that status.
