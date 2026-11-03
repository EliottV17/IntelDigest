# Python migration — M10: public destination validation policy
Branch: `migration/python-safe-scraper`; base and last reviewed boundary: `a077e4a4062586c2fce1dd1518c2219660bcfedf`; runner: `backend/.venv/bin/python -m pytest`.
Scope: M10 only (`inteldigest.scraper.models` and `inteldigest.scraper.policy`), inline implementation plus independent verification (`gentle-ai-verify`) and local commit. Risk: item 2 (security: SSRF public destination policy). Delivery: `auto-chain` / `feature-branch-chain`; forecast M10 about 400 authored lines. No M11 (`network.py`), M12 (`fetcher.py`), or M13 (`extraction.py`).

## Specs

- S1: User authorization: «continuemos con la migración, B2 de plan-migration quedó cerrada, ya pusheada y ahora estamos en main, lee el plan de migración e inicia M10 únicamente, finalizas con un commit y un resumen de lo que se implementó sin dar continuación a M11».
- S2: Authorized PLAN-MIGRATION.md M10 (branch `B3` — `migration/python-safe-scraper`): «Política de sintaxis, hosts, puertos, rangos IP y formatos ambiguos; casos permitidos y rechazados» with proposed commit `feat(scraper): preserve public destination validation`. Covers C7 (URL/IP/hostname/port syntax policy without DNS) and C8 (safe classified `ScrapeError` diagnostics without leaking causes or raw URLs in `str(err)`).
- S3: Approved package layout and scope boundary: create only `backend/src/inteldigest/scraper/{__init__,models,policy}.py` and unit tests under `backend/tests/unit/scraper/{test_models,test_policy}.py`, plus documentation updates in `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md`. Do not create `network.py`, `fetcher.py`, or `extraction.py` ahead of M11–M13; do not modify Go files, SQL migrations, or dependencies; do not read or load dotenv files or start services.

## Tasks

- T1 [S1–S3] — inline — done — Implemented `inteldigest.scraper.models` and `inteldigest.scraper.policy` plus unit tests (`test_models.py`, `test_policy.py`) and docs. RED: 2 collection errors before modules existed; GREEN: 125 focused passed, 471 suite passed / 12 skipped, Ruff clean.
- T2 [S1–S3] — delegated `gentle-ai-verify` — done — Task `mw01ogqa-1-nj2r`: PASS S1–S3 and overall PASS, 0 blockers; 125 focused / 471 suite passed (12 skipped), Ruff and `git diff --check` clean.
- T3 [S1–S3] — inline — done — Final parent spot-check passed (471 passed / 12 skipped, Ruff clean, `git diff --check` clean); M10 committed on `migration/python-safe-scraper` without starting M11.

## Log

### L1 — User request

continuemos con la migración, B2 de plan-migration quedó cerrada, ya pusheada y ahora estamos en main, lee el plan de migración e inicia M10 únicamente, finalizas con un commit y un resumen de lo que se implementó sin dar continuación a M11

### L2 — Reconciled starting evidence and approach

- Branch `migration/python-safe-scraper` created from `main` at `a077e4a4062586c2fce1dd1518c2219660bcfedf` (merge of B2 `#7`). Working tree clean.
- Scope is strictly M10: scraper error/domain models (`models.py`) and public destination URL/IP/hostname/port policy (`policy.py`) without DNS resolution or network dialing.
- Risk classification: item 2 (security — SSRF destination/IP policy and safe error formatting). Requires independent verification via `gentle-ai-verify` after self-verification and before closing/committing.

### L3 — Inline implementation and self-verification

- RED: `.venv/bin/python -m pytest tests/unit/scraper -q` failed with `ModuleNotFoundError: No module named 'inteldigest.scraper.models'` (exit 2) before `models.py` and `policy.py` were created.
- Implemented `backend/src/inteldigest/scraper/models.py` (`Article`, `ScrapeStats`, `ScrapeErrorKind`, `ScrapeStage`, `ScrapeError`) and `backend/src/inteldigest/scraper/policy.py` (`SecurityRejectedError`, `is_public_address`, `is_legacy_ipv4_form`, `valid_dns_name`, `is_metadata_hostname`, `valid_dial_port`, `valid_dial_host`, `validate_url_policy`), plus updated `backend/README.md`, `README.md`, and `PLAN-MIGRATION.md`.
- GREEN: `.venv/bin/python -m pytest tests/unit/scraper -q` passed 125 tests; `.venv/bin/python -m pytest -q` passed 471 tests (12 PostgreSQL integration skipped); `.venv/bin/ruff check src tests` passed (`All checks passed!`). No shared pre-existing production code was modified.

### L4 — Independent verification routing

- `gentle_review` `assess` with `escalate: {"item": 2, "reason": "SSRF public destination, IP range, and URL syntax validation policy in scraper/policy.py"}` returned `risk: "high"`, `independentVerifier: true`.
- Launched `gentle-ai-verify` in background as task `mw01ogqa-1-nj2r` to verify S1–S3 and security item 2 edge cases. Awaiting completion notification before T3 closure and commit.

### L5 — Independent verification result and parent spot-check

- Verifier `mw01ogqa-1-nj2r` returned PASS for S1, S2, and S3 with 0 blockers (125 focused tests passed, 471 suite passed / 12 skipped, Ruff clean, `git diff --check` clean).
- Proceeding to T3 parent spot-check, commit of M10 (`feat(scraper): preserve public destination validation`), and post-commit RDD assessment.

### L6 — Final parent spot-check and M10 commit

- Parent spot-check: `cd backend && .venv/bin/python -m pytest -q && .venv/bin/ruff check src tests && cd .. && git diff --check` exited 0 (`471 passed, 12 skipped`, `All checks passed!`).
- Committing M10 with `feat(scraper): preserve public destination validation` on `migration/python-safe-scraper` from base `a077e4a4062586c2fce1dd1518c2219660bcfedf`. No M11 work started.
