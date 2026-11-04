# Python migration — M15: port bounded concurrency and graceful drain
Branch: `migration/python-worker`; base and last reviewed boundary: `b3b458eb9152dc607d26fb49760fc4fce706935f`; runner: `backend/.venv/bin/python -m pytest`.
Scope: M15 only (`inteldigest.worker.pool`, package exports, unit tests, and documentation), delegated writer (`gentle-ai-worker`) plus independent verification (`gentle-ai-verify`) and local commit; stop before M16. Risk: item 4 (concurrency — fixed pool without accumulating queue, admission closure vs confirmed job drain timer, fatal `NOGROUP` propagation, and joining all tasks before returning). Delivery: `auto-chain` / `feature-branch-chain`; M14 slice ends at `b3b458eb9152dc607d26fb49760fc4fce706935f`; forecast M15 about 400 authored lines. No M16 (`__main__.py`).

## Specs

- S1: User authorization and stop boundary: «dale esta correcto, Continua con M15, terminas con un commit y la misma explicación que me diste de lo implementado la M14.».
- S2: Authorized PLAN-MIGRATION.md M15 (branch `B4` — `migration/python-worker`): «Concurrencia acotada sin cola acumulativa, presupuestos de operación y comportamiento requerido de admisión, drenaje/cancelación y cierre; seleccionar primitives Python idiomáticas, sin obligación de simular goroutines/channels» with proposed commit `feat(worker): port bounded concurrency and graceful drain`. Covers C6:
  - `PoolOptions` (`pool_size: int = 4`, `shutdown_timeout_seconds: float = 10.0`, `backoff_delay_seconds: float = 1.0`, `logger: logging.Logger | None = None`) and `default_pool_options()`: validates `1 <= pool_size <= 64`, finite positive `shutdown_timeout_seconds` and `backoff_delay_seconds`.
  - `QueueReader` (`async def read(self) -> Delivery | None`) and `JobProcessor` (`async def process(self, delivery: Delivery, *, admission_closed: asyncio.Event | None = None, job_canceled: asyncio.Event | None = None) -> tuple[Outcome, ProcessError | None]`) protocols; constructor `Pool(reader, processor, options=None)` rejects `None` dependencies or invalid options without opening or owning external resources.
  - Fixed concurrency without an accumulating in-memory queue: `await pool.run(stop_event: asyncio.Event | None = None)` starts exactly `pool_size` sequential worker tasks (`asyncio.Task`). While all `pool_size` workers are busy processing, no extra `reader.read()` calls are made; as each worker finishes a job, that slot reads the next delivery.
  - Single-run lifecycle & pre-stopped behavior: a `Pool` instance can only be run once (a second `run()` call raises `RuntimeError("worker pool can only be run once")`). If `stop_event` is already set (or `run()` is canceled before reading), `run()` exits cleanly without calling `reader.read()`.
  - Read results & error handling:
    - `None` (normal finite block timeout): worker immediately loops to read again while admission is open.
    - Fatal missing consumer group (`is_group_missing(err)` or `ConsumerGroupMissingError` anywhere in the exception cause chain): closes admission immediately, initiates shutdown drain, and after all worker tasks join, raises `PoolGroupMissingError("worker pool stopped: consumer group is missing")` with `__cause__` preserving the original error so `is_group_missing(err)` is `True` and `str(err)` never leaks sensitive outer causes.
    - Transient read error (when admission is still open): logs warning `worker_pool_read_failed code=transient` without leaking raw exception strings, and waits `backoff_delay_seconds` interruptibly by `admission_closed` (or cancellation) before retrying.
    - Late delivery after admission closes: if `reader.read()` returns a `Delivery` after `admission_closed` is set, the delivery is ignored and never passed to `processor.process`.
    - Per-job `processor.process` returning a non-`None` `ProcessError`: logs warning `worker_pool_job_failed code=processing` without leaking raw error details and continues without retrying.
  - Graceful shutdown & drain window: when `stop_event` is set, `pool.stop()` is called, or `run()` task cancellation / fatal `NOGROUP` occurs:
    1. `admission_closed` is set immediately (and any in-flight `reader.read()` or backoff wait is canceled/interrupted if needed, while respecting readers that complete or block). Note: in Python asyncio, canceling an in-flight `reader.read()` task when `admission_closed` fires—or letting `admission_closed` interrupt backoff while awaiting remaining tasks—must still honor the C6 drain contract: if a reader task is blocked (e.g. a custom fake reader ignoring cancellation until released), the drain timer (`shutdown_timeout_seconds`) starts as soon as admission closes and sets `job_canceled` when `shutdown_timeout_seconds` expires even while the blocked reader and confirmed job are still running, and `run()` waits for **all** worker tasks (both readers and processors) to finish before returning.
    2. Confirmed in-flight jobs receive `admission_closed` and `job_canceled` events; `job_canceled` is **not** set when admission closes—it is set only after `shutdown_timeout_seconds` elapses (or when all workers finish earlier).
    3. `Pool` never closes caller-owned `reader` or `processor` resources.
- S3: Approved package layout and scope boundary: create `backend/src/inteldigest/worker/pool.py` and `backend/tests/unit/worker/test_pool.py`, and update `backend/src/inteldigest/worker/__init__.py`, `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` to record M15 progress on `migration/python-worker`. Do not create `__main__.py` ahead of M16; do not modify Go files, SQL migrations, or dependencies; do not read or load dotenv files or start external services.

## Tasks

- T1 [S1–S3] — delegated `gentle-ai-worker` — done — Task `mw2jnrvh-2-k4uz`: implemented `inteldigest.worker.pool`, exports in `inteldigest.worker.__init__`, deterministic unit tests in `tests/unit/worker/test_pool.py` (RED observed `ModuleNotFoundError`, GREEN 15 focused / 594 suite passed, 12 skipped, Ruff clean, `git diff --check` clean), and updated `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` for M15.
- T2 [S1–S3] — delegated `gentle-ai-verify` — done — Task `mw2k678d-3-bvhr`: PASS S1–S3 and overall PASS, 0 blockers; 15 focused / 594 suite passed (12 skipped), Ruff and `git diff --check` clean.
- T3 [S1–S3] — inline — done — Final parent spot-check passed (594 passed / 12 skipped, Ruff clean, `git diff --check` clean); M15 committed on `migration/python-worker` without starting M16.

## Log

### L1 — User request

dale esta correcto, Continua con M15, terminas con un commit y la misma explicación que me diste de lo implementado la M14.

### L2 — Reconciled starting evidence and delegation

- Branch `migration/python-worker` is clean at reviewed M14 boundary `b3b458eb9152dc607d26fb49760fc4fce706935f`.
- Parent context is past the 150k-token context backstop, so T1 is delegated to `gentle-ai-worker` with narrow `Allowed edit surfaces`, followed by T2 independent verification (`gentle-ai-verify`, Risk: item 4 — concurrency).

### L3 — Worker completion and independent verification routing

- Worker `mw2jnrvh-2-k4uz` completed S1–S3: 15 focused pool tests passed, 594 total unit tests passed (12 PostgreSQL integration skipped), Ruff clean, `git diff --check` clean.
- Launching `gentle_review` `assess` and `gentle-ai-verify` in background for T2 before parent spot-check and commit.

### L4 — Independent verification and M15 commit

- Verifier `mw2k678d-3-bvhr` returned PASS for S1, S2, and S3 with 0 blockers (15 focused pool tests, 594 total unit tests / 12 skipped, Ruff clean, `git diff --check` clean).
- Parent spot-check passed (`594 passed, 12 skipped`, Ruff clean, `git diff --check` clean).
- Committing M15 (`feat(worker): port bounded concurrency and graceful drain`) on `migration/python-worker` against reviewed M14 boundary `b3b458eb9152dc607d26fb49760fc4fce706935f`. Stopping before M16.
