# IntelDigest Python package tooling

This directory contains an **experimental, installable Python backend package** with configuration, an async job repository, initial non-DNS URL validation, a FastAPI application with health, job lookup and submission endpoints, the Redis Stream v1 job publication contract, bounded Redis Stream consumption, and the safe scraper domain models, destination policy, network transport, bounded HTTP fetcher, and Readability extraction pipeline (M10–M13, completing branch B3). Worker execution (B4) is not implemented in Python yet; the Go backend remains operational. SQL migrations remain in the repository-root `migrations/` directory.

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

Branch B2 (`migration/python-redis-api`) completes the Redis publication and consumption contracts (M07–M09) and the FastAPI submission endpoint (M08) with deterministic unit/HTTP doubles. Running live Redis or PostgreSQL+HTTP instances remains unexecuted.

## Scraper models and public destination policy (M10)

`inteldigest.scraper.models` and `inteldigest.scraper.policy` implement the classified scraper error/domain types and strict non-DNS destination validation policy:

- `Article` (`text`, `requested_url`, `final_url`, `title`, `language`, `fetched_at`) and `ScrapeStats` (`bytes_read = 0`) represent extracted article metadata and per-call decompressed body byte counts.
- `ScrapeError(kind, stage, message="", *, http_status=0, cause=None)` retains `cause` and `__cause__` for programmatic inspection while `str(err)` emits only allowlisted classification tokens (`scrape <kind> during <stage>` or `scrape <kind> during <stage> (HTTP <status>)` when `100 <= http_status <= 599`), never exposing raw URLs, credentials, or underlying causes. Unknown `kind`/`stage` values render as `"unknown"`.
- `is_public_address(address)` enforces the IANA IPv4/IPv6 special-purpose address policy (RFC 6890 / RFC 8190), unmapping IPv4-mapped IPv6 addresses, blocking private/loopback/link-local/CGNAT/documentation/multicast/reserved prefixes, blocking Azure WireServer `168.63.129.16/32` while allowing adjacent public addresses (`168.63.129.15`, `168.63.129.17`), rejecting scoped IPv6 addresses, and restricting IPv6 to `2000::/3` global unicast outside `2001::/23`, `2002::/16`, `2001:db8::/32`, `3fff::/20`, `64:ff9b::/96`, and `64:ff9b:1::/48`.
- `is_legacy_ipv4_form(name)`, `valid_dns_name(host)`, `is_metadata_hostname(host)`, `valid_dial_port(port)`, and `valid_dial_host(host)` reject 1-to-4 component decimal/octal/hex numeric IPv4 grammars (`127.1`, `0177.0.0.1`, `0x7f000001`, `127.0x0.0.1`) and cloud/local metadata hostnames (`localhost`, `metadata`, `metadata.google.internal`, `metadata.google`, `instance-data`, `instance-data.ec2.internal`, `metadata.tencentyun.com`, `metadata.azure.com`, `metadata.aws.internal`, and subdomains) while allowing ordinary DNS labels containing `"0x"` (`0x.org`, `0xdeadbeef.example.com`).
- `validate_url_policy(raw_url)` enforces absolute `http`/`https` syntax, standard ports (`80` for `http`, `443` for `https`, rejecting leading zeros, empty ports, or cross-scheme ports), no credentials/userinfo, no percent-encoded or scoped hosts, and public IP literal or valid non-metadata DNS hostnames without performing DNS resolution or mutating the URL. Violations raise `ScrapeError(ScrapeErrorKind.SECURITY_REJECTED, ScrapeStage.VALIDATION)` with `SecurityRejectedError` as `__cause__`.

M10 is covered by deterministic unit tests (`.venv/bin/python -m pytest tests/unit/scraper/test_policy.py tests/unit/scraper/test_models.py -q`).

## Scraper network transport and connection pinning (M11)

`inteldigest.scraper.network` implements the safe network transport layer with coupled DNS resolution and public IP connection pinning:

- `SafeDialer` and `dial_public_address`: validate port (`80` or `443`), host, and metadata names before any resolution or dial. Share one connect timeout budget (`connect_timeout_seconds`, default 5.0s) across DNS resolution and TCP dial. IP literals are verified against public address policy and dialed directly without invoking DNS; hostnames are resolved fresh per connection without caching. Empty or mixed DNS answers (any non-public IP) cause immediate rejection (`ScrapeError(SECURITY_REJECTED, FETCH)`). Dial targets are pinned to the approved public IP literal while preserving the original hostname for TLS SNI and HTTP Host.
- `create_tls_context(*, cafile=None, capath=None, cadata=None)`: creates an `ssl.SSLContext` enforcing minimum TLS 1.2, `CERT_REQUIRED`, and strict hostname verification (`check_hostname=True`), supporting custom test root certificates.
- `SafeTCPConnector` (subclassing `aiohttp.TCPConnector`) and `create_safe_connector`: enforces fresh coupled resolution, public IP pinning, `force_close=True` (keep-alive disabled), `use_dns_cache=False` (DNS cache disabled), and prohibits HTTP proxies (`proxy` parameters are rejected with `SECURITY_REJECTED`).
- `create_safe_client_session`: creates an `aiohttp.ClientSession` using `SafeTCPConnector`, `aiohttp.DummyCookieJar()` (no persistent cookies), and `trust_env=False` (ignoring proxy environment variables).
- `classify_network_error(exc)`: classifies underlying exceptions into `ScrapeError(kind, FETCH)` (`TIMEOUT`, `CANCELED`, or `NETWORK`) while preserving the underlying exception in `cause`/`__cause__` and emitting safe non-leaking string messages (`"scrape network during fetch"`).

M11 is covered by deterministic unit tests (`.venv/bin/python -m pytest tests/unit/scraper/test_network.py -q`). Bounded HTTP fetching/redirects (M12) and Readability extraction (M13) complete branch B3; worker execution (B4) remains pending.

## Bounded HTTP fetcher and redirect safety (M12)

`inteldigest.scraper.fetcher` implements the safe, bounded HTTP fetcher enforcing redirect safety, timeout budgets, and decompressed body limits:

- `ScraperOptions` and `validate_scraper_options(options)`: explicit network and body bounds matching Go and configuration defaults (`connect_timeout_seconds=5.0`, `tls_timeout_seconds=5.0`, `header_timeout_seconds=10.0`, `read_timeout_seconds=10.0`, `request_timeout_seconds=30.0`, `max_body_bytes=5_242_880`, `max_redirects=5`, allowed range `0..10`). All timeouts must be finite positive numbers; `max_body_bytes` must be a positive integer `< sys.maxsize`; `max_redirects` must be in `0..10` (0 disables redirects).
- `FetchedPage` (`requested_url`, `final_url`, `status_code`, `content_type`, `charset`, `body`, `fetched_at`) and `Fetcher` / `SafeFetcher`: provide `async def fetch(raw_url: str) -> FetchedPage` and `async def fetch_with_stats(raw_url: str, *, stats: ScrapeStats | None = None) -> tuple[FetchedPage, ScrapeStats]` with async context manager support (`async with Fetcher() as fetcher:` and `await fetcher.close()`).
- SSRF redirect safety: validates the initial URL with `validate_url_policy` before any DNS lookup or dial (`stats.bytes_read == 0` on validation rejection). Uses manual per-hop execution (`allow_redirects=False`) without forwarding `Referer`, `Authorization`, or `Cookie`. Redirect responses (301, 302, 303, 307, 308) are closed immediately without draining bodies. Missing/empty `Location` raises `HTTP_STATUS`. Redirect counts exceeding `max_redirects` raise `ScrapeError(SECURITY_REJECTED, FETCH, "redirect limit exceeded")`. Targets are revalidated via `validate_url_policy` before issuing the next hop request or resolving DNS. HTTPS to HTTP downgrade is rejected as `ScrapeError(SECURITY_REJECTED, VALIDATION, "HTTPS downgrade rejected")`. Each redirect hop resolves DNS fresh through `SafeTCPConnector`. Non-redirect non-2xx status codes raise `ScrapeError(HTTP_STATUS, FETCH, http_status=status)`.
- Global request deadline & timeout budgets: a single global `request_timeout_seconds` deadline is enforced across all redirect hops and body reading combined (never resetting per hop). If the global deadline expires, `ScrapeError(TIMEOUT, FETCH, "scrape request timed out")` is raised. Per-hop headers must arrive within `header_timeout_seconds` and body read must finish within `read_timeout_seconds` (both bounded by remaining global time). External cancellation raises `ScrapeError(CANCELED, FETCH)`.
- Decompressed body limits & gzip safety: with `auto_decompress=False`, inspects `Content-Encoding`. If uncompressed (`""` or `"identity"`), rejects `Content-Length > max_body_bytes` immediately, and bounds streamed chunks to `max_body_bytes + 1`, raising `ScrapeError(BODY_TOO_LARGE, FETCH)` if the limit is exceeded. If `"gzip"` or `"x-gzip"`, streams compressed chunks and decompresses incrementally via `zlib.decompressobj(16 + zlib.MAX_WBITS)`, bounding decompressed output to `max_body_bytes + 1` bytes so gzip bombs expanding beyond the limit are aborted immediately without buffering unbounded memory. Corrupt or truncated gzip streams raise `ScrapeError(NETWORK, FETCH)`. Unsupported encodings (`"br"`, `"deflate"`, stacked `"gzip, br"`) raise `ScrapeError(UNSUPPORTED_CONTENT, FETCH)`.
- MIME type, 512-byte sniffing, and charset validation: if `Content-Type` is missing, the first 512 bytes (`CONTENT_SNIFF_BYTES`) of the decompressed body are sniffed for HTML markers (`<!doctype html`, `<html`, `<head`, `<body`, `<article`) after stripping UTF-8 BOM and whitespace (an oversized body `> max_body_bytes` fails with `BODY_TOO_LARGE`, winning over sniffing). If `Content-Type` is present, it is parsed strictly (defects/unclosed quotes raise `UNSUPPORTED_CONTENT`), allowing only `text/html` and `application/xhtml+xml`. Charset parameters are validated with `codecs.lookup(charset)`, rejecting unknown charsets as `UNSUPPORTED_CONTENT`.
- `ScrapeStats.bytes_read`: remains 0 if any validation, connection, redirect, status, MIME/charset check, or body read fails; once the final decompressed response body is completely read and validated, `stats.bytes_read = len(body)`.

M12 is covered by deterministic unit tests (`.venv/bin/python -m pytest tests/unit/scraper/test_fetcher.py -q`).

## Article extraction, charset decoding, and safe Scraper pipeline (M13)

`inteldigest.scraper.extraction` implements article readability extraction, post-wire charset decoding, safe classified diagnostics, and the end-to-end `Scraper` pipeline:

- `ExtractedArticle` (`text`, `title`, `language`) and `ArticleExtractor` protocol: represents intermediate extraction results and allows injecting custom synchronous or asynchronous extractors for testing (`(html_text: str, page_url: str = "") -> ExtractedArticle | Awaitable[ExtractedArticle]`).
- `decode_html_bytes(body: bytes, declared_charset: str | None = None) -> str`: decodes raw wire bytes into Unicode string.
  - If `declared_charset` is provided and non-empty, validates it via `codecs.lookup(candidate)` and decodes `body` using `errors="replace"`. An unknown charset name raises `ScrapeError(ScrapeErrorKind.UNSUPPORTED_CONTENT, ScrapeStage.FETCH, "HTML charset is unsupported")`.
  - If `declared_charset` is absent or empty: checks UTF-8/UTF-16 BOMs first (`\xef\xbb\xbf`, `\xff\xfe`, `\xfe\xff`), then inspects the first 1024 bytes of `body` for `<meta charset="...">` or `<meta http-equiv="Content-Type" content="...; charset=...">` declarations. If found, validates the codec name with `codecs.lookup` (raising `UNSUPPORTED_CONTENT` if unknown) and decodes; otherwise defaults to UTF-8 (`body.decode("utf-8", errors="replace")`).
  - Wire limit safety: charset decoding occurs strictly **after** decompressed `max_body_bytes` is enforced by `Fetcher`. Single-byte encodings (such as ISO-8859-1 `0xe9` -> `"é"`) expanding to multi-byte UTF-8 at exact `max_body_bytes` wire length reach the extractor safely without failing `BODY_TOO_LARGE`. `ScrapeStats.bytes_read` retains the decompressed wire byte count even if extraction subsequently fails.
- `extract_readability(html_text: str, page_url: str = "") -> ExtractedArticle`:
  - Uses `readability.Document` (`readability-lxml`) to produce the article summary HTML (`doc.summary(html_partial=True)`).
  - Cleans the summary with `lxml.html`: removes `<script>`, `<style>`, and `<noscript>` elements, converts `<br>` to line breaks, adds paragraph separation (`\n\n`) around block elements (`<p>`, `<div>`, `<article>`, `<h1>`-`<h6>`, etc.), collapses consecutive empty lines, and strips surrounding whitespace.
  - Extracts `title`: prefers `doc.short_title()` or `doc.title()`, falling back to `<title>` or `<h1>` from the original document tree.
  - Extracts `language`: inspects the document root `lang`, `xml:lang`, or `{http://www.w3.org/XML/1998/namespace}lang` attributes, defaulting to `""` if absent.
  - On fixture `article.html`: extracts title `"Harbor Rescue Report"`, language `"en"`, retains editorial content (`"twelve sailors"`, `"engine stopped"`), and cleanly excludes navigation/advertising clutter (`"Subscribe"`, `"Limited time offer"`, `"must not appear"`).
  - On fixture `no_article.html`: produces empty text (`""`), which causes `Scraper` to reject the page as `ScrapeError(ScrapeErrorKind.EXTRACTION, ScrapeStage.EXTRACT, "page contains no useful article text")`.
  - Wraps any unexpected parser or extraction exception into `ScrapeError(ScrapeErrorKind.EXTRACTION, ScrapeStage.EXTRACT, "article extraction failed", cause=exc)` with safe string representation (`"scrape extraction during extract"`), never leaking internal URLs, tokens, or document content.
- `Scraper`:
  - Provides `async def scrape(raw_url: str) -> Article` and `async def scrape_with_stats(raw_url: str, *, stats: ScrapeStats | None = None) -> tuple[Article, ScrapeStats]` with async context manager support (`async with Scraper(...) as scraper:` and `await scraper.close()`).
  - Tracks the global `request_timeout_seconds` budget across fetching, charset decoding, and extraction combined. If the global budget expires during extraction, raises `ScrapeError(ScrapeErrorKind.TIMEOUT, ScrapeStage.FETCH, "scrape request timed out")`. Cancellation raises `ScrapeError(ScrapeErrorKind.CANCELED, ScrapeStage.FETCH, "scrape request was canceled")`.
  - Non-blocking execution: synchronous extractors (including `extract_readability`) are offloaded to a worker thread via `await asyncio.to_thread(...)` bounded by `asyncio.timeout(remaining_budget)`, ensuring CPU-bound `lxml` parsing does not block the asyncio event loop.
  - Normalizes `fetched_at` to UTC datetime.
  - `ScrapeStats.bytes_read` contract: remains 0 on early validation or connection failure; retains decompressed wire byte count when the response body is fully fetched, even if charset decoding or article extraction later fails.

### Extractor differences between Go and Python

- **Text rendering pipeline:** Go's `go-readability` (`codeberg.org/readeck/go-readability/v2`) parses the DOM and renders text directly via its internal `article.RenderText(w)` method. Python's `readability-lxml` produces a partial HTML fragment (`doc.summary(html_partial=True)`), which is parsed by `lxml.html` to extract plain text with block-level double newlines (`\n\n`) and line-level whitespace normalization. Both implementations produce equivalent semantic text and exclude clutter on `article.html`.
- **Language detection:** Go's `go-readability` exposes `article.Language()` from the HTML root. In Python, `readability-lxml` does not expose a dedicated language property; language is resolved by inspecting `lang` and `xml:lang` on the root `<html>` element parsed with `lxml.html`, returning `"en"` on `article.html`.
- **Concurrency & event loop safety:** Go executes extraction within its concurrent goroutine model. Python offloads synchronous `readability-lxml`/`lxml` parsing to a worker thread via `asyncio.to_thread`, keeping the asyncio event loop responsive while strictly enforcing the global `request_timeout_seconds` deadline.

Branch B3 (`migration/python-safe-scraper`) is completed with M10–M13. Worker execution (B4 / M14–M16) remains pending.

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
  scraper/
    __init__.py
    extraction.py
    fetcher.py
    models.py
    policy.py
    network.py
tests/
  conftest.py
  fixtures/
    scraper/
      article.html
      no_article.html
  unit/
    test_config.py
    test_package.py
    jobs/{test_repository,test_validation}.py
    api/test_app.py
    queue/{test_message,test_publisher,test_consumer}.py
    scraper/{test_models,test_policy,test_network,test_fetcher,test_extraction}.py
  integration/
    conftest.py
    test_repository.py
```

`config.py` stays transversal; job models, SQL and URL validation live together in `jobs/`. HTTP schemas live in `api/`, separate from the persisted job model. `queue/` holds the M07 message/publisher modules and M09 consumer module; `scraper/` holds the M10 error/domain models and public destination policy (`models.py`, `policy.py`), the M11 safe network transport layer (`network.py`), the M12 bounded HTTP fetcher (`fetcher.py`), and the M13 Readability extraction and Scraper pipeline (`extraction.py`). Scraper fixtures live in `tests/fixtures/scraper/`. Worker modules are not created ahead of their tasks (B4).

Unit tests require no services. PostgreSQL fixtures live only in `tests/integration/conftest.py`; the root `conftest.py` merely registers the explicit connection option so pytest can recognize it consistently. Pytest uses `--import-mode=importlib` to collect identically named unit/integration test modules without package-name collisions or modifying `sys.path`.

## Package status and boundary

The package uses `src/` layout and has an inert `inteldigest` package initializer. Its smoke test checks installed metadata and imports the selected runtime dependencies. The HTTP API is wired to configuration, database repository and Redis publisher through its lifespan; `inteldigest.queue` provides v1 message serialization, `XADD` publication, and bounded stream consumption, not yet wired to a standalone worker process or scraper.

Generated virtualenv, uv cache, build output, pytest cache, and Python bytecode are local artifacts and ignored by Git. The existing Go implementation and its commands remain the operational reference until a separately authorized migration task changes that status.
