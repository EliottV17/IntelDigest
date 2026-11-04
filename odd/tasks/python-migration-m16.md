# Python migration — M16: wire standalone Python worker lifecycle
Branch: `migration/python-worker`; base and last reviewed boundary: `b7097fe5a7d1bdedb67cdc191aa0afc0ccc24672`; runner: `backend/.venv/bin/python -m pytest`.
Scope: M16 only (`inteldigest.worker.__main__`, package exports, unit tests, and documentation closing branch B4), delegated writer (`gentle-ai-worker`) plus independent verification (`gentle-ai-verify`) and local commit; stop before B5. Risk: item 4 (concurrency — signal registration before dependency startup, consumer group creation before reading, ordered shutdown after pool join, and safe non-leaking exit codes/errors). Delivery: `auto-chain` / `feature-branch-chain`; M15 slice ends at `b7097fe5a7d1bdedb67cdc191aa0afc0ccc24672`; forecast M16 about 400 authored lines. No B5 (`tests/integration/test_worker.py` etc.).

## Specs

- S1: User authorization and stop boundary: «dale termina con M16 según en plan-migration y terminas con un commit, me dices lo nuevo que tiene y como probarlo en algun cliente de api(si puedes probarlo con curl o algo parecido, lo haces me dices que de nuevo te arroja), ojo solo si las implementaciones de esta B4 tuvieron que ver con esa parte de agregar endpoints, sino una descripcion es mas que suficiente.».
- S2: Authorized PLAN-MIGRATION.md M16 (closing branch `B4` — `migration/python-worker`): «Entrada de proceso independiente, señales antes de iniciar dependencias, grupo antes de leer y cierre después del join» with proposed commit `feat(worker): wire standalone Python worker lifecycle`. Covers C5, C6, C8, C10:
  - Standalone entrypoint `python -m inteldigest.worker` (`backend/src/inteldigest/worker/__main__.py`) with `main() -> int`, `run(deps: WorkerDependencies | None = None, *, stop_event: asyncio.Event | None = None) -> int`, and `run_with_dependencies(deps: WorkerDependencies | None = None, *, stop_event: asyncio.Event | None = None) -> None`.
  - Safe coded errors `WorkerStartupError(code: str, cause: BaseException | None = None)`: `str(err)` returns only `"worker: <code>"` while retaining `cause` and `__cause__` so `is_group_missing(err)` and cause inspection work without leaking `DATABASE_URL`, `REDIS_URL`, stream names, or payloads. `exit_code(err)` returns `0` on `None` and `1` on any error; `exit_code_label(err)` returns `err.code` for `WorkerStartupError` and `"worker_failed"` otherwise.
  - Strict lifecycle ordering:
    1. **Register signals first (`SIGINT`, `SIGTERM`):** signal handlers (setting `stop_event`) are registered **before** loading configuration or opening any database, Redis, or scraper resources, and unregistered exactly once in `finally` on exit.
    2. **Load worker configuration (`load_worker`):** maps all validated `WorkerConfig` fields (`database_url`, `redis_url`, `redis_stream`, `redis_dial_timeout_seconds`, `pool_size`, `consumer_group`, `consumer_name`, `stream_block_seconds`, `queue_operation_timeout_seconds`, `db_timeout_seconds`, `shutdown_timeout_seconds`, `scraper_*`). Rejects `None` config or missing dependencies with `WorkerStartupError`.
    3. **Open and verify PostgreSQL (`new_db` + ping):** opens the database handle/pool bounded by `db_timeout_seconds` (honoring `stop_event`), checks `stop_event`, and verifies connectivity (`ping` / `SELECT 1`) under an independent `db_timeout_seconds` budget. If `new_db` raises or fails after returning a handle, any acquired handle is closed.
    4. **Open Redis consumer and ensure consumer group (`new_consumer` + `ensure_group`):** opens `Consumer` with `WorkerConfig` settings, checks `stop_event`, and calls `await consumer.ensure_group()` bounded by `queue_operation_timeout_seconds` **before** creating or running the pool.
    5. **Initialize `Scraper`, `Repository`, `Processor` (with `MetadataReceiver`), and `Pool`:** maps `ScraperOptions`, `ProcessorOptions`, and `PoolOptions` (`pool_size`, `shutdown_timeout_seconds`, `backoff_delay_seconds=1.0`, logger with `consumer_group` and `consumer_name` context without logging `database_url`, `redis_url`, or `redis_stream`). Logs `worker_started` with `consumer_group`, `consumer_name`, and `pool_size`.
    6. **Run pool and join before closing resources:** awaits `pool.run(stop_event)`. Only **after** `pool.run` returns (all workers joined) does cleanup close ` scraper` (if applicable), `consumer` (`aclose`/`close`), and `database` (`close`) in order: Redis consumer before PostgreSQL database. An error closing the consumer is wrapped into `WorkerStartupError("consumer_close_failed", exc)` and **never skips closing the database**.
    7. **Early stop/failure short-circuit:** if `stop_event` is set between any startup steps, or any factory/check fails, later factories are **never** called, and already-acquired resources are closed in reverse order (`consumer` then `database`).
- S3: Approved package layout and scope boundary: create `backend/src/inteldigest/worker/__main__.py` and `backend/tests/unit/worker/test_main.py`, and update `backend/src/inteldigest/worker/__init__.py`, `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` to close branch B4 (`migration/python-worker`). Do not start B5 integration harnesses; do not modify Go files, SQL migrations, or dependencies; do not read or load dotenv files or start external services.

## Tasks

- T1 [S1–S3] — delegated `gentle-ai-worker` — done — Task `mw2o3xy5-4-2ng2`: implemented `inteldigest.worker.__main__`, exports in `inteldigest.worker.__init__`, deterministic unit tests in `tests/unit/worker/test_main.py` (RED observed `ImportError`, GREEN 41 focused / 635 suite passed, 12 skipped, Ruff clean, `git diff --check` clean), and updated `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` to close B4.
- T2 [S1–S3] — delegated `gentle-ai-verify` — done — Task `mw2on80b-5-474b`: PASS S1–S3 and overall PASS, 0 blockers; 41 focused / 635 suite passed (12 skipped), Ruff and `git diff --check` clean.
- T3 [S1–S3] — inline — done — Final parent spot-check passed (635 passed / 12 skipped, Ruff clean, `git diff --check` clean); M16 committed on `migration/python-worker`, closing branch B4 without starting B5.

## Log

### L1 — User request

dale termina con M16 según en plan-migration y terminas con un commit, me dices lo nuevo que tiene y como probarlo en algun cliente de api(si puedes probarlo con curl o algo parecido, lo haces me dices que de nuevo te arroja), ojo solo si las implementaciones de esta B4 tuvieron que ver con esa parte de agregar endpoints, sino una descripcion es mas que suficiente.

### L2 — Reconciled starting evidence and delegation

- Branch `migration/python-worker` is clean at reviewed M15 boundary `b7097fe5a7d1bdedb67cdc191aa0afc0ccc24672`.
- Delegating T1 to `gentle-ai-worker` with narrow `Allowed edit surfaces`, followed by T2 independent verification (`gentle-ai-verify`, Risk: item 4 — concurrency and resource lifecycle).

### L3 — Worker completion and independent verification routing

- Worker `mw2o3xy5-4-2ng2` completed S1–S3: 41 focused main lifecycle tests passed, 635 total unit tests passed (12 PostgreSQL integration skipped), Ruff clean, `git diff --check` clean.
- Launching `gentle_review` `assess` and `gentle-ai-verify` in background for T2 before parent spot-check and commit.

### L4 — Independent verification and M16 commit (closing B4)

- Verifier `mw2on80b-5-474b` returned PASS for S1, S2, and S3 with 0 blockers (41 focused main lifecycle tests, 635 total unit tests / 12 skipped, Ruff clean, `git diff --check` clean).
- Parent spot-check passed (`635 passed, 12 skipped`, Ruff clean, `git diff --check` clean).
- Committing M16 (`feat(worker): wire standalone Python worker lifecycle`) on `migration/python-worker` against reviewed M15 boundary `b7097fe5a7d1bdedb67cdc191aa0afc0ccc24672`. This closes branch B4. No B5 work started.
