# Python migration — M12: preserve bounded fetch and redirect safety
Branch: `migration/python-safe-scraper`; base and last reviewed boundary: `7d86d850f19823b8626a474ff07ccb25dada2db6`; runner: `backend/.venv/bin/python -m pytest`.
Scope: M12 only (`inteldigest.scraper.fetcher`), delegated writer (`gentle-ai-worker` via context backstop) plus independent verification (`gentle-ai-verify`) and local commit. Risk: item 2 (security: SSRF redirect revalidation, HTTPS downgrade rejection, header/read/TLS/request timeout budgets, and decompressed body / gzip bomb limits). Delivery: `auto-chain` / `feature-branch-chain`; M11 slice ends at `7d86d850f19823b8626a474ff07ccb25dada2db6`; forecast M12 about 400 authored lines. No M13 (`extraction.py`) or B4 (`worker/`).

## Specs

- S1: User authorization: «genial. Confirmo la continuación con la implementación de M12 de plan de migración, al finalizar terminas con commit, y resumen de la implementación».
- S2: Authorized PLAN-MIGRATION.md M12 (branch `B3` — `migration/python-safe-scraper`): «Descarga con redirecciones revalidadas, downgrade rechazado y presupuestos de tiempo/cuerpo descomprimido» with proposed commit `feat(scraper): preserve bounded fetch and redirect safety`. Covers C7 (manual redirect handling with `allow_redirects=False`, destination revalidation via `validate_url_policy` before issuing next hop request, HTTPS->HTTP downgrade rejection, fresh DNS validation per redirect hop via `SafeTCPConnector`, no `Referer` header forwarding across redirects, redirect response body closed without unbounded drain) and C8 (explicit `ScraperOptions` validation and `default_options()`, connect 5s, TLS 5s, headers 10s, read 10s, global request 30s shared across all hops, max decompressed body 5,242,880 bytes with exact max accepted and max+1 rejected as `body_too_large` across sniff/read and gzip decompression, corrupt gzip rejected as `network`, non-2xx HTTP status rejected as `http_status` with status code, unsupported Content-Encoding / MIME / charset rejected as `unsupported_content` or sniffed within 512 bytes when `Content-Type` is absent, and `ScrapeStats.bytes_read` recording decompressed final-response body bytes only after a complete bounded read).
- S3: Approved package layout and scope boundary: create only `backend/src/inteldigest/scraper/fetcher.py` and `backend/tests/unit/scraper/test_fetcher.py`, and update `backend/src/inteldigest/scraper/__init__.py`, `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md` (plus `backend/src/inteldigest/scraper/network.py` only if a small seam is needed for TLS handshake timeout or test dialer). Do not create `extraction.py` ahead of M13; do not modify Go files, SQL migrations, or dependencies; do not read or load dotenv files or start external services.

## Tasks

- T1 [S1–S3] — delegated `gentle-ai-worker` — done — Task `mw04n7t0-4-tu3s`: implemented `inteldigest.scraper.fetcher` and `test_fetcher.py`, updated docs; RED observed `ModuleNotFoundError`, GREEN 179 focused / 525 suite passed (12 skipped), Ruff and `git diff --check` clean.
- T2 [S1–S3] — delegated `gentle-ai-verify` — done — Task `mw056x24-5-bsil`: PASS S1–S3 and overall PASS, 0 blockers; 179 focused / 525 suite passed (12 skipped), Ruff and `git diff --check` clean.
- T3 [S1–S3] — inline — done — Final parent spot-check passed (525 passed / 12 skipped, Ruff clean, `git diff --check` clean); M12 committed on `migration/python-safe-scraper` without starting M13.

## Log

### L1 — User request

genial. Confirmo la continuación con la implementación de M12 de plan de migración, al finalizar terminas con commit, y resumen de la implementación

### L2 — Reconciled starting evidence and approach

- Branch `migration/python-safe-scraper` is clean at reviewed M11 boundary `7d86d850f19823b8626a474ff07ccb25dada2db6`.
- Parent context is past the 150k-token context backstop, so T1 implementation is delegated to `gentle-ai-worker` with narrow `Allowed edit surfaces`.
- Risk classification: item 2 (security — redirect SSRF revalidation, HTTPS downgrade rejection, global request / per-stage timeout budgets, and bounded decompressed body / gzip handling). T2 independent verification via `gentle-ai-verify` will run after T1 completes.

### L3 — Delegated T1 implementation to gentle-ai-worker

- Launched `gentle-ai-worker` in background as task `mw04n7t0-4-tu3s` with `Allowed edit surfaces` restricted to `backend/src/inteldigest/scraper/{__init__,fetcher,network}.py`, `backend/tests/unit/scraper/test_fetcher.py`, `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md`. Awaiting completion event.

### L4 — Worker completion and independent verification routing

- Worker `mw04n7t0-4-tu3s` completed S1–S3: 179 focused scraper tests passed, 525 total unit tests passed (12 PostgreSQL integration skipped), Ruff clean, `git diff --check` clean.
- `gentle_review` `assess` with `escalate: {"item": 2, "reason": "SSRF redirect revalidation, HTTPS downgrade rejection, header/read/TLS/request timeout budgets, and decompressed body / gzip bomb limits in scraper/fetcher.py"}` returned `risk: "high"`, `independentVerifier: true`.
- Launching `gentle-ai-verify` in background for T2 before parent spot-check and commit.

### L5 — Independent verification result and M12 commit

- Verifier `mw056x24-5-bsil` returned PASS for S1, S2, and S3 with 0 blockers (179 focused scraper tests passed, 525 total unit tests passed / 12 skipped, Ruff clean, `git diff --check` clean).
- Running parent spot-check, committing M12 (`feat(scraper): preserve bounded fetch and redirect safety`), and assessing against reviewed M11 boundary `7d86d850f19823b8626a474ff07ccb25dada2db6`. No M13 work started.
