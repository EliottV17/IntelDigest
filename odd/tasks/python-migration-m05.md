# Python migration — M05: atomic claims and bounded diagnostics
Branch: `migration/python-storage-api`; base and last reviewed boundary: `5f069ef5b19539125c881a465d8114e18f8a2ed9`; runner: `backend/.venv/bin/python -m pytest`.
Scope: M05 only, inline implementation plus independent verification. Risk: item 4 (concurrent claims). Delivery: existing `auto-chain` / `feature-branch-chain`; M04 slice ends at the base; forecast M05 about 400 authored lines. No M06. Isolated PostgreSQL execution, PostgreSQL-only startup, create-if-missing test database and the M05 commit are authorized under S4–S5; preserve application data and volumes.

## Specs

- S1: User authorization: «genial, continua con la implementación del M05».
- S2: Authorized PLAN-MIGRATION.md M05: «Claim atómico y diagnóstico intermedio condicionado y acotado; pruebas del único ganador y estado conservado». Contract C3: «Mantener tabla, UUID, timestamps, nulos y JSONB; claim mediante `UPDATE ... WHERE status = 'pending'`, con un solo ganador».
- S3: Existing approved structure: «M05 amplía el repositorio existente; M06 inicia `api/`». No M06 implementation, Go/schema/dependency changes or dotenv access. No application database changes, database/volume deletion or unrelated service operations. Only the PostgreSQL startup/test-database creation in S5 and the test-owned schema/transaction effects in S4 are authorized.
- S4: User authorization: «autorizo la ejecución de la integración aislada de PostgreSQL». Run the existing opt-in test suite serially only against a configured, dedicated local `inteldigest_test`; rollback ordinary test transactions and create/drop only the contention test's owned `m05_<uuid>` schema. Do not assume credentials, a pre-existing database or a shared-instance grant.

- S5: User authorization: «Autorizo levantar solo PostgreSQL con Compose sin cargar archivos dotenv y crear inteldigest_test si no existe, sin borrar datos ni volúmenes. Hazlo y al final si terminas con m05 haces el commit, y me dices que es lo que llevamos hasta ahora y que ya puede hacer el proyecto en lo que llevamos de migración, que estoy un poco perdido».

## Tasks

- T1 [S1–S3] — inline — done — SQL methods, opt-in integration cases and docs implemented. RED: 16 failed / 34 passed; GREEN: 50 focused, 99 suite / 12 skipped; Ruff and diff checks passed. Included in the M05 work-unit commit named in L10.
- T2 [S1–S3] — delegated `gentle-ai-verify` — done — Task `mvx0e3uq-1-4kwq`: PASS S1–S3 for implementation, no blockers; 50 focused / 99 suite passed, 12 integration cases collected and skipped at that stage. Later T4 verifies real PostgreSQL contention.
- T3 [S1–S3] — inline — done — Initial handoff before S4–S5: 99 passed / 12 skipped, Ruff and diff checks exit 0. Subsequent authorized integration/commit are recorded in T4–T5; no M06.
- T4 [S4–S5] — inline — done — PostgreSQL started without dotenv; test database created only because absent; 12 real serial integration cases passed, exit 0; schema/table/job-count baseline restored. No application data/volume deletion.
- T5 [S5] — inline — done — Authorized M05 commit created successfully with final checks and documentation; 99 unit tests and 12 real PostgreSQL cases passed. Final commit identity is recorded in Git/Engram; no M06, push or PR.

## Log

### L1 — User request

genial, continua con la implementación del M05

### L2 — Reconciled starting evidence and approach

- Clean worktree, M04 commit `5f069ef5b19539125c881a465d8114e18f8a2ed9`; exact M04 native review acknowledged in prior turn. RDD enabled.
- Add `try_mark_processing(job_id)` as one conditional UPDATE clearing the old error; add `record_scrape_error(job_id, safe_reason)` as one processing-only UPDATE with a 1024-character limit. Use `RETURNING id` / row presence with existing asyncpg `fetchrow`; no Go command-tag emulation or read-before-write.
- Preserve status, result and unrelated fields on diagnostic updates. Missing/noneligible rows return false; errors and cancellation propagate without retries/logging. The caller must supply an already-safe diagnostic, not article/secret text.
- Allowed edit surfaces: `backend/src/inteldigest/jobs/repository.py`, `backend/tests/unit/jobs/test_repository.py`, `backend/tests/integration/{conftest,test_repository}.py`, `backend/README.md`, `PLAN-MIGRATION.md`, this ledger. No new product packages or dependencies.
- PostgreSQL integration cases may be authored but must remain opt-in; tests requiring services are not authorized to run. Do not represent a fake-based single-winner test as proof of real PostgreSQL contention.

### L3 — Inline implementation and observed checks

- RED: `.venv/bin/python -m pytest tests/unit/jobs/test_repository.py -q` gave 16 failed, 34 passed, exit 1 before adding methods (AttributeError). Unit parameter IDs were subsequently shortened to avoid oversized Unicode failure output.
- Implemented the two SQL writes using the existing `fetchrow` protocol; no existing create/lookup helper changes. A differential check is not applicable: no pre-existing shared production helper/parser was modified.
- GREEN: focused 50 passed; full Python suite 99 passed, 12 skipped; Ruff `All checks passed!`; `git diff --check` and protected Go/SQL/Makefile/dependency diff both exit 0. Initial Ruff SIM117 on nested test contexts corrected before the final clean run.
- Integration covers all four states, missing rows, field/timestamp preservation, Unicode truncation and two real connections synchronized by `asyncio.Barrier`. It has not run. The contention fixture creates a UUID-named schema in dedicated local `inteldigest_test`, applies unchanged SQL there and drops only that schema; no database create/drop. These effects require explicit future authorization.
- T1 implementation/self-check is complete; T2 independent verification begins now. No commit, services, migrations, dotenv access or M06.

### L4 — Independent verification routing

- Native ASSESS: nativeRisk medium, agent escalation item 4/concurrency applied, risk high, self-verification and independent verifier required. `reviewDue=false`, `reviewDueReason=under_budget`; no native review START is due for this candidate. This is not a human decision to leave it unreviewed.
- Verifier `mvx0e3uq-1-4kwq` launched in background against the M04 base. Scope includes per-spec implementation checks and explicit separation of fake/static proof from real PostgreSQL integration, which is not authorized. Completion is event-driven; no polling.

### L5 — Independent evidence reviewed by parent

- Verifier returned PASS for S1–S3, no implementation blockers. Isolated scratch commands: Ruff exit 0 (`All checks passed!`); focused pytest 50 passed, exit 0; suite 99 passed / 12 skipped, exit 0; integration collection 12 items, exit 0. It also reports deterministic probes for Unicode/code-point bounds, rejected/missing-row preservation and M04 behavior, exit 0. These are simulated/unit results, not actual PostgreSQL observations.
- Parent qualification: the verifier's explanation overgeneralizes `READ COMMITTED` behavior to stronger isolation levels. A competing transaction can raise a serialization error at stronger isolation; repository exceptions propagate and there are no automatic retries. Claim success is also provisional until any caller-owned transaction commits. No source correction is needed.
- A character cap is a Unicode code-point cap, not a guarantee that grapheme clusters stay intact. `updated_at` changes on successful writes; rejected writes preserve all fields. No guarantee of strictly increasing timestamps across arbitrary transactions is added.
- Real single-winner PostgreSQL integration is still unexecuted and requires explicit authorization for the documented isolated test effects. M05 implementation verification is complete within that limitation; T3 closure/parent spot-check follows.

### L6 — Parent spot-check and implementation handoff

- Parent reran `.venv/bin/python -m pytest -q`: 99 passed, 12 skipped, exit 0; `.venv/bin/ruff check src tests`: `All checks passed!`, exit 0. Both diff checks and protected Go/SQL/Makefile/dependency diff exit 0. No new functional failures or code correction needed after independent verification.
- README qualifies default isolation, propagated serialization errors, caller-owned commit boundaries and Unicode code points. PLAN-MIGRATION records independent verification complete but does not mark live integration complete.
- Implementation and in-scope available checks are done. The 12 skipped cases include the real single-winner test; no PostgreSQL/Redis, network, Compose or migrations were run. No native review approval is claimed for M05; the earlier assessment offered no review start (`reviewDue=false`).
- Human decision at this handoff: authorize the documented real PostgreSQL integration separately, or confirm committing M05 with that limitation recorded. No commit/M06/push/PR has been executed. Existing code and test changes remain staged for review; M04 is still HEAD.

### L7 — Integration authorized; connection configuration missing

autorizo la ejecución de la integración aislada de PostgreSQL

- Authorization covers the existing serial PostgreSQL integration tests and their documented isolated transaction/schema effects; not starting services, creating/dropping a database, touching application data, reading dotenv or committing M05.
- Read-only preflight: current fixtures/tests inspected; `psql` and `pg_isready` available. `DATABASE_URL`, `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and `PGPASSWORD` are unset in the agent environment. No credential values were displayed and no files outside the project or dotenv files were accessed.
- No connection URL or authenticated local test target had been provided. T4 was blocked awaiting explicit configuration plus confirmation of a prepared dedicated `inteldigest_test`; no database connection, migration or test was attempted and no claim of readiness was made.

### L8 — Authorized local PostgreSQL preparation and M05 commit

Autorizo levantar solo PostgreSQL con Compose sin cargar archivos dotenv y crear inteldigest_test si no existe, sin borrar datos ni volúmenes. Hazlo y al final si terminas con m05 haces el commit, y me dices que es lo que llevamos hasta ahora y que ya puede hacer el proyecto en lo que llevamos de migración, que estoy un poco perdido

- Resumed from matching ledger/Engram mirror and Git base. Compose declares PostgreSQL 17, host port 5432, application database `inteldigest`, persistent `pgdata`; no test database declaration. Credentials come only from the explicitly inspected/authorized Compose definition, never dotenv.
- Every Compose invocation will clear `COMPOSE_ENV_FILES` and set `COMPOSE_DISABLE_ENV_FILE=1`, using the explicit repository Compose file. Start only `postgres` with no dependency startup and no container recreation. Never delete volumes/databases or change application rows.
- Create `inteldigest_test` only if absent, run the existing pytest integration serially and retain PostgreSQL afterward (no shutdown authorized). New commit is now explicitly authorized only on successful M05 checks; no push, PR or M06.

### L9 — Observed live PostgreSQL integration

- Startup job-1 exit 0: Compose up `postgres --no-deps --no-recreate`; container `inteldigest-postgres-1` created/started, internal `pg_isready` accepted connections. All Compose calls removed `COMPOSE_ENV_FILES` and set `COMPOSE_DISABLE_ENV_FILE=1`, with an explicit repository file. No Redis startup or dotenv access.
- Job-2 exit 0: admin `psql -X` issued `CREATE DATABASE inteldigest_test` only after catalog absence check. The test database was absent and was created; no database or volume was dropped. Local credentials were obtained from authorized Compose configuration only in subprocess memory, not printed or written to repository files.
- Exact test form: `.venv/bin/python -m pytest tests/integration/test_repository.py -v --tb=short --test-database-url postgresql://<local-user>@127.0.0.1:5432/inteldigest_test`, with password supplied only in subprocess environment. Result: **12 passed in 0.21s**, exit 0, no test failures. Scope includes M04 creation/lookup and M05 conditional writes, rejected/missing states, JSONB/Unicode/timestamps and two concurrent real connections yielding one winner.
- Before/after schema names, public table names and job count matched. All test-owned schema/transaction effects were cleaned up. This metadata check is not an assertion of a byte-for-byte application-data backup; no test connected to the application database to write data.
- PostgreSQL and the new test database are intentionally left available. No Redis/HTTP/scraper/AI integration, service shutdown, M06, commit or publishing has run yet. T4 done; T5 final documentation/checks/commit in progress.

### L10 — Final pre-commit checks and delivery boundary

- Current final checks: `.venv/bin/python -m pytest tests/unit -q` gave **99 passed**, exit 0; Ruff `All checks passed!`, exit 0. Live integration is the separately observed **12 passed** from L9; no skipped applicable PostgreSQL proof remains for M04/M05.
- `git diff --check`, cached diff check and the protected Go/SQL/Makefile/dependency diff all exit 0. No existing production helper was modified; M04 methods remain unchanged. Only final documentation was edited after independent verification/live integration.
- Root README now links the migration/package docs and distinguishes Python's implemented configuration/repository from the still-Go runnable pipeline. This is a current-status note, not an API/worker implementation or a product-plan rewrite.
- Delivery slices remain separate work-unit commits: M04 ends at `5f069ef`; this M05 unit is based on that reviewed boundary, not the accumulated feature diff. Existing `auto-chain` / `feature-branch-chain` applies to eventual publication; no push or PR is authorized now. The single M05 commit message is `feat(db): preserve atomic claims and scrape diagnostics`; final observed SHA belongs in the Engram mirror after Git returns, not a self-referential source line.
- Final pre-commit ASSESS: native medium, concurrency escalation item 4 applied, `reviewDue=false` / `under_budget`; required self-check and independent verification already passed. No native review approval is claimed. Git commit succeeded; the completion ledger is folded into that same unpublished local work unit, not a separate bookkeeping commit.
