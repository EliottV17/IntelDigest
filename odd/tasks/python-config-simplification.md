# Python configuration simplification — M03 correction
Scope: Python configuration only; branch `migration/python-foundation-config`, base `f6eaa5cdc82197dc3887f42d993093afa417da54`; no M04.
Delivery: existing auto-chain / feature-branch-chain strategy; one cohesive correction commit; pytest without services. Forecast: ~500–800 authored lines, mainly replacing Go-emulation tests and deleting parsers, not expanding behavior.

## Specs

- S1: «como habiamos acordado, el objetivo de la migración es garantizar el mismo funcionamiento, no simular estrictamente el funcionamiento de go usando python, que la migración sea unicamente usando lo que ofrece python/fastapi y las dependencias acordadas, no haciendo tecnicas raras para simular el funcionamiento nativo de go, primero el config, quitando complejidades y tecnicas extrañas para simular lo que se hizo en go.»
- S2: User selected «Segundos numéricos — recomendado»: «Ejemplos: 5 y 0.5. Valores float en segundos, sin parser de duraciones propio.»

## Tasks

- T1 [S1–S2] — done; delegated worker mvvsk3ql-1-knlq (runtime context-backstop); observed RED exit 1 (86 pass, 3 fail), GREEN focused 48 pass / full 49 pass, diff-check exit 0; delivery unit `refactor(config): use idiomatic Python settings`, based on `f6eaa5cdc82197dc3887f42d993093afa417da54`.
- T2 [S1–S2] — done; independent verifier mvvsrcgx-2-1qf1 PASS S1/S2, isolated suite 49 pass plus six public probe groups; parent spot check focused48/full49 exit0 and scope/whitespace exit0; delivery unit same as T1. Actual commit identity is recorded in the Engram mirror after observing the commit; no self-referential SHA.

## Log

### L1 — Original request

como habiamos acordado, el objetivo de la migración es garantizar el mismo funcionamiento, no simular estrictamente el funcionamiento de go usando python, que la migración sea unicamente usando lo que ofrece python/fastapi y las dependencias acordadas, no haciendo tecnicas raras para simular el funcionamiento nativo de go, primero el config, quitando complejidades y tecnicas extrañas para simular lo que se hizo en go.

### L2 — Confirmed duration representation

User selected: 1. Segundos numéricos — recomendado (value: numeric_seconds)

- Effective durations remain unchanged; Python environment values use seconds (`5`, `0.5`), no Go duration parser, nanoseconds or int64 emulation. Library-native URL parsing replaces the Go-compatible manual parser.
- Preserve passive `load_api`/`load_worker`, explicit mappings or process environment, separate settings, useful bounds, safe errors, required URLs and effective defaults. No dotenv, DNS, network/services, dependency additions, Go changes or M04.
- Allowed implementation surfaces: `backend/src/inteldigest/config.py`, `backend/tests/test_config.py`, `backend/README.md`, root `README.md` only Python configuration note, `docs/migration/compatibility-baseline.md` only approved Python deviations appended without rewriting historical Go evidence.
- `PLAN-MIGRATION.md` is human-owned untracked input and remains untouched; its strict Go-duration criterion is superseded for Python by S1–S2. Original B0 ledger remains historical evidence.
- RED/GREEN required through public loaders: numeric duration rule, retained defaults and API/worker separation, relevant limits, library-native URL handling and safe diagnostics. Remove tests whose only purpose is reproducing Go internals; retain product-level regressions.
- Risk: item 3 (shared config helpers affect both existing public loaders beyond duration representation); independently verify the retained shared behavior. No credentials or service calls.
- One coherent correction may exceed the approximate 400-line heuristic due to deleting/replacing the Go emulation and associated tests. Do not minify, omit meaningful tests, or split code from its tests/docs to reduce the count.
- No push, PR, merge or operational changes. Local commit only after required checks and review routing; no amend of historical M03.

### L3 — Writer delivery and parent readback

- Worker delivered five scoped files: config, its tests, backend README, root README and appended baseline exception; 186 insertions / 369 deletions, 555 authored changed lines.
- Worker RED exit 1: 86 pass / 3 fail, numeric seconds and minimum block rejected by old parser, nan accepted. Final focused 48 pass and full 49 pass, exit 0; intermediate Redis path regressions were corrected before delivery. No services, installs or dotenv access reported.
- Parent read config and diff; Go remains unchanged. Native ASSESS returned nativeRisk unassessable due to untracked input; explicit item-3 escalation requires independent verification, not native approval. No review started.
- T1 is complete based on writer tests and readback; at this point T2 was still open pending independent verification and parent spot check.

### L4 — Verification complete

- Independent verifier mvvsrcgx-2-1qf1: PASS S1/S2; isolated backend suite 49 passed in 0.27s, exit 0. Public-loader probes: defaults, numeric seconds/boundaries, integer bounds, URL/credential safety, isolation/immutability, shared-helper consistency all PASS, exit 0.
- Parent spot check: focused 48 passed in 0.05s and full 49 passed in 0.27s; combined command exit 0, no error output. `git diff --check` and `git diff --exit-code <base> -- cmd internal migrations Makefile go.mod go.sum backend/pyproject.toml backend/uv.lock` both exit 0.
- No in-scope functional blockers. Integration/services and live end-to-end operation were not run or authorized. Differential check N/A: both public Python consumers are in scope, no other Python consumers exist, Go code unchanged. Context7 was unavailable to the writer; actual installed redis-py parsing was exercised by tests and independent probes.
- Native assessment unavailable due to the untracked input; no native approval is claimed. The mandatory independent item-3 verifier completed successfully. Native lifecycle was not started because ASSESS returned no reviewDue or START continuation.
- Correction unit: five behavior/tests/docs files plus this ledger, one local commit `refactor(config): use idiomatic Python settings`; actual SHA retained in memory once the commit command returns successfully. Human untracked plan excluded; no push/PR/merge. Stop before M04.
