# Python migration — M14: preserve job processing and acknowledgement rules
Branch: `migration/python-worker`; base and last reviewed boundary: `d50c9bbf1e61aaef10478345871efb6d253e94c8`; runner: `backend/.venv/bin/python -m pytest`.
Scope: M14 only (`inteldigest.worker.{receiver,processor,__init__}` and unit tests), inline implementation plus independent verification (`gentle-ai-verify`) and local commit; stop before M15 per user instruction. Risk: item 4 (concurrency — atomic claim winner vs loser requery, admission closing vs confirmed job draining, job cancellation semantics, and C5 non-ACK guarantees). Delivery: `auto-chain` / `feature-branch-chain`; forecast M14 about 400 authored lines. No M15 (`pool.py`) or M16 (`__main__.py`).

## Specs

- S1: User authorization and stop boundary: «arraquemos con B4 de plan-migration.md, ya hice el pr, checkout main y pull, B3 quedó cerrada. Empieza creando la nueva rama que está en el .md e inicias con M14 cuando termines me da una explicación por encima de lo que se implementó(no técnico ni con código, puedes usar analogias, algunos fragmentos de codigo para explicarlo, o si hay conceptos importantes una explicación y una analogía para entenderlo si es necesario) y no procedas con M15 sin que te confirme».
- S2: Authorized PLAN-MIGRATION.md M14 (opening branch `B4` — `migration/python-worker`): «Procesador: claim, duplicados, errores y receiver síncrono de metadatos; ningún ACK normal ni estado terminal nuevo» with proposed commit `feat(worker): preserve job processing and acknowledgement rules`. Covers C3 (atomic claim via `try_mark_processing` where only one concurrent caller wins, pre-claim lookup via `get_job_by_id`, requery on claim loss, and conditional bounded diagnostic persistence via `record_scrape_error`), C4 (strict decoding and validation of `Delivery` field `data` JSON object with `schema_version=1`, `job_id`, and `url`, rejecting missing/non-string/malformed/non-object/invalid payloads without touching DB, scraper, receiver, or ACK), C5 (ACK via `ack(entry_id)` only when the stored job is proven terminal `completed` or `failed` before claim or on claim-loss requery; never ACK `processing` duplicates, claim losers still `pending`, scrape successes, scrape failures, receiver failures, or operational cancellations; leave jobs in `processing` with no new terminal status), C6 (admission vs job lifetime separation: if admission closes before or during pre-claim lookup or claim, return `Outcome.SKIPPED` with `ProcessError("admission_closed")` without claiming, scraping, or ACKing; once a job wins the claim, admission closure does not cancel it and it drains to completion; if job cancellation occurs before or during processing, return `Outcome.OPERATIONAL_CANCELED` with `ProcessError("job_context_canceled")` without recording diagnostics, delivering results, or ACKing; enforce `db_timeout_seconds` and `queue_operation_timeout_seconds` per operation), and C8 (safe static `ProcessError` strings `"worker processing failed: <code>"` that retain `__cause__`/`cause` without leaking secrets, URLs, or payloads; safe diagnostic reason `"scraping/<kind>: HTTP <status>"` or `"scraping/<kind> during <stage>"` persisted via `record_scrape_error`; error classification `"permanent"`, `"transient"`, or `"unknown"` using allowlisted kinds, HTTP statuses, and certificate/network causes; synchronous `Result(job_id, article, error)` validation requiring a non-nil UUID and exactly one of `article` or `error`; and `MetadataReceiver` / `Receiver` logging only safe metadata without article text, titles, or URLs).
- S3: Approved package layout and scope boundary: create `backend/src/inteldigest/worker/__init__.py`, `backend/src/inteldigest/worker/receiver.py`, `backend/src/inteldigest/worker/processor.py`, `backend/tests/unit/worker/__init__.py`, and `backend/tests/unit/worker/test_processor.py`, and update `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` to record M14 progress on `migration/python-worker`. Do not create `pool.py` (M15) or `__main__.py` (M16); do not modify Go files, SQL migrations, or dependencies; do not read or load dotenv files or start external services.

## Tasks

- T1 [S1–S3] — inline — done — Implemented `inteldigest.worker.{receiver,processor,__init__}`, deterministic unit tests in `tests/unit/worker/test_processor.py` (RED observed `ModuleNotFoundError`, GREEN 24 focused / 579 suite passed, 12 skipped, Ruff clean, `git diff --check` clean), and updated `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` for M14.
- T2 [S1–S3] — delegated `gentle-ai-verify` — done — Task `mw2hn1fo-1-2972`: PASS S1–S3 and overall PASS, 0 blockers; 24 focused / 579 suite passed (12 skipped), Ruff and `git diff --check` clean.
- T3 [S1–S3] — inline — done — Final parent spot-check passed (579 passed / 12 skipped, Ruff clean, `git diff --check` clean); M14 committed on `migration/python-worker` without starting M15.

## Log

### L1 — User request

arraquemos con B4 de plan-migration.md, ya hice el pr, checkout main y pull, B3 quedó cerrada. Empieza creando la nueva rama que está en el .md e inicias con M14 cuando termines me da una explicación por encima de lo que se implementó(no técnico ni con código, puedes usar analogias, algunos fragmentos de codigo para explicarlo, o si hay conceptos importantes una explicación y una analogía para entenderlo si es necesario) y no procedas con M15 sin que te confirme

### L2 — Branch creation and starting evidence

- Checked `main` at `d50c9bbf1e61aaef10478345871efb6d253e94c8` (PR #8 merged for B3).
- Created and checked out branch `migration/python-worker` for B4.
- Scoped M14 to `inteldigest.worker.{receiver,processor,__init__}` and `tests/unit/worker/test_processor.py`, plus documentation updates. Will stop after M14 and wait for user confirmation before M15.

### L3 — T1 implementation and checks

- Observed RED (`ModuleNotFoundError: No module named 'inteldigest.worker'`) on `backend/tests/unit/worker/test_processor.py`.
- Implemented `backend/src/inteldigest/worker/receiver.py`, `backend/src/inteldigest/worker/processor.py`, and `backend/src/inteldigest/worker/__init__.py`.
- Updated `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` for M14.
- Observed GREEN: `cd backend && .venv/bin/python -m pytest -q` passed 579 tests (24 in `test_processor.py`), 12 PostgreSQL integration skipped; `.venv/bin/ruff check src tests` passed with 0 errors; `git diff --check` clean.
- Launching `gentle-ai-verify` for T2 independent verification (Risk: item 4 — concurrency and atomic claim/ACK rules).

### L4 — Independent verification and M14 commit

- Verifier `mw2hn1fo-1-2972` returned PASS for S1, S2, and S3 with 0 blockers (24 focused worker tests, 579 total unit tests / 12 skipped, Ruff clean, `git diff --check` clean).
- Parent spot-check passed (`579 passed, 12 skipped`, Ruff clean, `git diff --check` clean).
- Committing M14 (`feat(worker): preserve job processing and acknowledgement rules`) on `migration/python-worker` against base `d50c9bbf1e61aaef10478345871efb6d253e94c8`. Stopping before M15 per user instruction.
