# Worker and safe scraper implementation

## Objective and authorization

Implement the approved `PLAN.md`: consume Redis Streams, atomically claim pending jobs, bound concurrency, fetch public HTML safely and deliver article/error output without pretending jobs are terminal. User authorized strict TDD, branch `feat/worker-scraper`, and Conventional Commits per work unit; no push, PR creation or merge is authorized.

## Scope and constraints

- Preserve queue Message/API/schema contracts; no AI, terminal persistence, retries, PEL recovery or generic status updates.
- Scrape failures remain `processing` with a safe `error` diagnosis, no ACK and no recovery, even without a crash. Cancellation is operational, not a definitive business failure.
- A single 30s deadline spans the initial request and all redirects. Six fresh connections could otherwise exhaust 60s in connect/TLS alone; each stage is capped by remaining time. Keep-alive/HTTP2/proxies are disabled.
- Only proven durable terminal duplicates are ACKed, using Stream entry IDs.
- Single writer, exactly N persistent goroutines, bounded inputs, no text/URL/credential logging or abandoned parser goroutines.
- Keep unrelated untracked `.codegraph/` out of commits. Existing PLAN.md changes are the approved plan, not disposable edits.

## Work units

Stable IDs below are the authority for the visible todo projection. Source tasks are delegated to `gentle-ai-worker` (multi-file or preparation trigger); the parent owns docs/tracking, commits, review and reconciliation. No task is complete until required evidence and its work-unit commit exist.

| ID | Status | Deliverable | Allowed edit surfaces | Checks and acceptance |
|---|---|---|---|---|
| WS-00 | in_progress | Record approved plan and implementation units | `PLAN.md`, `odd/tasks/worker-scraper.md` | Structural readback, `git diff --check`; documentation has no meaningful RED |
| WS-01 | pending | Atomic processing claim and safe intermediate diagnostic | `internal/db/repository.go`, `internal/db/repository_unit_test.go`, `internal/db/repository_test.go` | Observed RED/GREEN; `go test ./internal/db`, race and real PostgreSQL claim race/terminal protection |
| WS-02 | pending | Worker-only validated configuration and environment examples | `internal/config/config.go`, `internal/config/config_test.go`, `.env.example` | Observed RED/GREEN; `go test ./internal/config`; API Load compatibility, overflow/duration/name validation |
| WS-03 | pending | Bounded Redis consumer operations | `internal/queue/consumer.go`, `internal/queue/consumer_test.go`, `internal/queue/consumer_integration_test.go` | Observed RED/GREEN; `go test ./internal/queue`; exact group/read/ACK semantics, cancellation and real Redis PEL |
| WS-04 | pending | Public URL/IP policy and DNS-safe dialing | `internal/scraper/types.go`, `internal/scraper/network.go`, `internal/scraper/network_test.go` | Observed RED/GREEN; `go test ./internal/scraper`; all special/private/metadata/mapped/ambiguous addresses, mixed DNS, literal dial pinning |
| WS-05 | pending | Deadline-bounded HTTP fetch and useful HTML extraction | `go.mod`, `go.sum`, `internal/scraper/types.go`, `internal/scraper/network.go`, `internal/scraper/scraper.go`, `internal/scraper/scraper_test.go`, `internal/scraper/testdata/*` | Observed RED/GREEN; `go test ./internal/scraper`, race; redirects, cumulative deadline, body/gzip/MIME bounds and extraction; pinned compatible readability dependency |
| WS-06 | pending | Claim-gated job processing and synchronous output contract | `internal/worker/worker.go`, `internal/worker/receiver.go`, `internal/worker/worker_test.go` | Observed RED/GREEN; `go test ./internal/worker`; invalid payload/DB mismatch/claim outcomes, terminal-only ACK, diagnosis and complete output |
| WS-07 | pending | Fixed pool, Redis backpressure and two-stage shutdown | `internal/worker/pool.go`, `internal/worker/pool_test.go`, `internal/worker/worker.go` | Observed RED/GREEN; `go test -race ./internal/worker`; channel-coordinated N limit, no extra reads, cancelable backoff, NOGROUP fatal and resource-safe drain |
| WS-08 | pending | Testable CLI startup and signal wiring | `cmd/worker/main.go`, `cmd/worker/main_test.go`, `README.md` | Observed RED/GREEN for deterministic startup boundaries; `go test ./cmd/worker`, `go build ./cmd/api ./cmd/worker`; initialization failure cleanup, no sensitive logging |
| WS-09 | pending | Cross-component integration and final operational documentation | `internal/worker/worker_integration_test.go`, `README.md`, `Makefile` | Integration additions test-first where behavior is not already implemented; `go test ./...`, `go test -race ./...`, `go build ./cmd/api ./cmd/worker`, `go test -tags=integration ./internal/db ./internal/queue ./internal/worker`; duplicate exclusion, failure persistence, restart/non-recovery and new-message progress |

Documentation lives with the behavior it describes. If a coherent unit naturally exceeds the advisory 400-line planning heuristic, explain the review load; do not minify, omit tests or split by file type. Scope corrections must update this document and its mirror before the writer continues.

## Strict TDD and verification

Write runnable tests first, record the exact failing assertion or missing API observed before implementation, then minimal GREEN and refactor with focused checks passing. Use deterministic fakes, httptest with test-only synthetic public resolution/literal-IP routing, and channel/barrier coordination; never use real internet in tests. Each bounded writer runs parent-authorized verification commands synchronously and reports exact outcomes. No invented RED/GREEN evidence. External integration runners may be unavailable; record failures/skips explicitly and keep incomplete verification pending.

PostgreSQL integration must use the existing isolated `inteldigest_test` convention and independent connections against a committed row to demonstrate exactly one winning claim. Redis uses isolated stream/group names with cleanup. The compose configuration alone does not prove services are running; verify readiness safely before real integration tests. No production targets, implicit destructive setup or indiscriminate cleanup.

## Review and delivery boundaries

- Branch point / first review base: `0f7dab6`.
- RDD: on (global; read-only `gentle-ai review mode status`). Assess each work-unit commit, inspect before offered START, follow only provider continuations. High/unassessable candidates are reviewed immediately; medium candidates may be reviewed at slice close; passive docs use structural checks.
- Planning forecast: approximately 2,500–3,500 authored changed lines, including tests and docs, excluding generated files. This is an estimate, not an acceptance target.
- Delivery strategy: `ask-on-risk`; user selected `chain_strategy=feature-branch-chain`. Keep `feat/worker-scraper` as the integrator and group reviewable work-unit commits into dependent slices. No PRs or publishing will be executed without separate authorization.
- Running committed authored line count: 0. Planned slices: approval/tracking; DB claims; configuration; consumer; network policy; HTTP/extraction; processing; pool/shutdown; CLI; integration. Exact commit boundaries and native tier assessments will refine this grouping.
- Keep source, tests and relevant docs together; every task records commit ID, checks, runtime proof (or justified N/A), review tier/outcome and rollback boundary.

## Progress and evidence

### WS-00 — in progress

- Applied both requested clarifications to PLAN.md and marked it approved.
- Created branch `feat/worker-scraper` from `0f7dab6`.
- `git diff --check -- PLAN.md`: PASS.
- Read-only exploration `mut0yobq-1-rghf`: mapped repository and suggested units. Its clean-tree assertion was inaccurate: parent readback confirms modified PLAN.md and unrelated untracked `.codegraph/`; preserve both.
- No source writes, tests, integration readiness check or implementation commits yet.
- Runtime proof: N/A, documentation/tracking only. Rollback: approved plan/tracking documentation only, no runtime behavior.

## Next step

The user selected the feature/tracker branch chain. Commit approved plan/tracking as WS-00, then delegate WS-01 with strict TDD. Maintain one in-progress task; synchronize local document, full Engram mirror `odd/worker-scraper/tasks` and visible todo after each transition.
