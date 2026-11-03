# Python migration — M13: port article extraction and safe diagnostics
Branch: `migration/python-safe-scraper`; base and last reviewed boundary: `4a397831f3724e0e6bde8a2a0949a6e7196beebb`; runner: `backend/.venv/bin/python -m pytest`.
Scope: M13 only (`inteldigest.scraper.extraction` and end-to-end `Scraper.scrape` / `Scraper.scrape_with_stats`, closing branch B3), delegated writer (`gentle-ai-worker`) plus independent verification (`gentle-ai-verify`) and local commit. Risk: item 2 (security: safe classified diagnostics without leaking cause/URLs/content, bounded extraction, and C7–C9 scraper closure). Delivery: `auto-chain` / `feature-branch-chain`; M12 slice ends at `4a397831f3724e0e6bde8a2a0949a6e7196beebb`; forecast M13 about 400 authored lines. No B4 (`worker/`).

## Specs

- S1: User authorization: «dale ya lo revisé, ahora para finalizar con esta rama termina el M13, trata de usar un poco más de subagentes exclusivamente para esta parte, luego olvidas esta regla que te digo, debido a que ya se va a acabar la cuota de consumo de este orchestrator, al terminar me mandas un resumen de lo implementado y terminas con un commit».
- S2: Authorized PLAN-MIGRATION.md M13 (closing branch `B3` — `migration/python-safe-scraper`): «Extracción Readability, charset, metadatos y errores tipados; fixtures comparadas y ejecución acotada» with proposed commit `feat(scraper): port article extraction and safe diagnostics`. Covers C9 (extract useful article text, title, and language from HTML using `readability-lxml` and `lxml`, exclude navigation/advertising clutter on `article.html`, reject empty/non-article pages like `no_article.html` as `ScrapeError(ScrapeErrorKind.EXTRACTION, ScrapeStage.EXTRACT)`, decode declared or HTML meta charsets such as `ISO-8859-1` `café` after enforcing wire/decompressed `max_body_bytes` so charset expansion beyond `max_body_bytes` still reaches the extractor and `ScrapeStats.bytes_read` retains the original decompressed wire byte count even if extraction later fails, normalize `fetched_at` to UTC, and run synchronous Readability/lxml parsing without blocking the event loop via `asyncio.to_thread` bounded by the remaining global `request_timeout_seconds`) and C8 (wrap any extractor/charset exception into `ScrapeError` with `cause` preserved and safe `str(err)` never exposing raw URLs, tokens, or article content).
- S3: Approved package layout and scope boundary: create `backend/src/inteldigest/scraper/extraction.py`, `backend/tests/fixtures/scraper/{article.html,no_article.html}` (matching `internal/scraper/testdata/{article.html,no_article.html}`), and `backend/tests/unit/scraper/test_extraction.py`, and update `backend/src/inteldigest/scraper/{__init__,fetcher}.py`, `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` to close B3. Do not create `worker/` modules ahead of B4; do not modify Go files, SQL migrations, or dependencies; do not read or load dotenv files or start external services.

## Tasks

- T1 [S1–S3] — delegated `gentle-ai-worker` — done — Task `mw06bnye-6-5gnr`: implemented `inteldigest.scraper.extraction`, fixtures, `test_extraction.py`, and updated docs for B3 closure; RED observed `ModuleNotFoundError`, GREEN 209 focused / 555 suite passed (12 skipped), Ruff and `git diff --check` clean.
- T2 [S1–S3] — delegated `gentle-ai-verify` — done — Task `mw06xw7r-7-owih`: PASS S1–S3 and overall PASS, 0 blockers; 209 focused / 555 suite passed (12 skipped), Ruff and `git diff --check` clean; fixtures byte-for-byte parity confirmed.
- T3 [S1–S3] — inline — done — Final parent spot-check passed (555 passed / 12 skipped, Ruff clean, `git diff --check` clean); M13 committed on `migration/python-safe-scraper`, closing B3.

## Log

### L1 — User request

dale ya lo revisé, ahora para finalizar con esta rama termina el M13, trata de usar un poco más de subagentes exclusivamente para esta parte, luego olvidas esta regla que te digo, debido a que ya se va a acabar la cuota de consumo de este orchestrator, al terminar me mandas un resumen de lo implementado y terminas con un commit

### L2 — Reconciled starting evidence and approach

- Branch `migration/python-safe-scraper` is clean at reviewed M12 boundary `4a397831f3724e0e6bde8a2a0949a6e7196beebb`.
- Delegating T1 (exploration of `internal/scraper/testdata/`, implementation of `extraction.py`, fixtures, unit tests, and documentation updates) to `gentle-ai-worker`, followed by T2 independent verification via `gentle-ai-verify`.

### L3 — Delegated T1 implementation to gentle-ai-worker

- Launched `gentle-ai-worker` in background as task `mw06bnye-6-5gnr` with `Allowed edit surfaces` restricted to `backend/src/inteldigest/scraper/{__init__,extraction,fetcher}.py`, `backend/tests/fixtures/scraper/{article,no_article}.html`, `backend/tests/unit/scraper/test_extraction.py`, `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md`. Awaiting completion event.

### L4 — Worker completion and independent verification routing

- Worker `mw06bnye-6-5gnr` completed S1–S3: 209 focused scraper tests passed, 555 total unit tests passed (12 PostgreSQL integration skipped), Ruff clean, `git diff --check` clean.
- Launching `gentle-ai-verify` in background for T2 before parent spot-check and commit.

### L5 — Session resumed after orchestrator model quota change

continua con la tarea que quedó pendiente antes de que se acabará la cuota del anterior modelo, no cortes los subagentes corriendo

- Resumed with verifier `mw06xw7r-7-owih` still running; it was not cancelled or relaunched. One status check only at the decision boundary.

### L6 — Independent verification result and M13 commit

- Verifier `mw06xw7r-7-owih` returned PASS for S1, S2, and S3 with 0 blockers (209 focused scraper tests, 555 total unit tests / 12 skipped, Ruff clean, `git diff --check` clean, fixtures byte-for-byte identical to `internal/scraper/testdata/`).
- Left unverified by design: live network scraping and real external services; deterministic fakes and fixtures only. The 12 skipped cases are the opt-in PostgreSQL integration suite from M05.
- Committing M13 (`feat(scraper): port article extraction and safe diagnostics`) against reviewed M12 boundary `4a397831f3724e0e6bde8a2a0949a6e7196beebb`. This closes branch B3. No B4 work started.
