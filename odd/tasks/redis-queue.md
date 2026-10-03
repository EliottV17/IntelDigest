# Redis queue and API publishing

## Objective
Publish each newly persisted digest job to Redis Streams from the API, with a stable message contract and explicit failure behavior, ready for a later worker implementation.

## Problem and rationale
`POST /api/v1/digests` currently creates a PostgreSQL job and responds without queueing it. The approved `PLAN.md` selects Streams to retain unacknowledged deliveries, while acknowledging that API→PostgreSQL→Redis is not atomic and safely reclaiming `processing` jobs requires a worker-owned lease/heartbeat design.

## Scope and constraints
- Implement only the approved first follow-up task: queue publication from API; no consumer or processing behavior.
- Keep Redis Streams message contract as in `PLAN.md` (`schema_version`, `job_id`, `url`); PostgreSQL remains source of truth.
- On publish failure after persistence, return 503 including the created `job_id`; document the ambiguous XADD acknowledgement / duplicate POST risk without adding idempotency support.
- No retries/outbox/dead-letter queue/worker lease in this unit.
- English technical artifacts; Go standard library preferred and dependencies justified.
- Work on `feat/redis-queue`; commit each coherent checklist task conventionally after checks.

## Checklist
1. [x] Amend the approved plan with the requested 503 `job_id` body and ambiguous XADD acknowledgement/duplicate POST risk.
2. [x] Add Redis connection/publisher package and configuration with focused contract/config tests. User authorized `github.com/redis/go-redis/v9@v9.7.3`; TDD RED/GREEN observed.
3. [x] Inject publisher into API POST flow and wire `cmd/api`; add test-first coverage for ordering, success, and 503 error body containing `job_id`.
4. [x] Add build-tagged Redis integration coverage and update README runtime documentation; verify against Redis Compose service.
5. [x] Update `.env.example` with Redis variables/defaults. User attested the exact non-secret values; committed as `00a0964 chore(config): document Redis environment settings`. The file itself was not directly inspected due the safety block.

## Acceptance criteria
- API persists then publishes before returning 202; failed publish returns 503 with `{ "error": "...", "job_id": "<uuid>" }` and does not pretend persistence rolled back.
- Stream entry `data` follows the approved versioned JSON contract.
- API startup validates Redis configuration and connectivity within bounded timeout; Redis resources close on shutdown.
- Unit tests need no Redis; integration tests are explicitly tagged and documented.
- No worker consumption, processing retries, or outbox implementation is introduced.

## Progress and evidence
- Authorized by user after approval of `PLAN.md`, with two requested amendments.
- Branch: `feat/redis-queue`.
- Route: delegated bounded multi-file writer (trigger: implementation spans multiple files); parent owns plan, checklist, reconciliation, assessment, and commits.
- Plan amendment verified by reading `PLAN.md`; no code tests applicable to this documentation-only update.
- User explicitly authorized exact dependency command `go get github.com/redis/go-redis/v9@v9.7.3`; it was used for the maintained Streams client rather than hand-rolling Redis protocol.
- Item 2 TDD evidence: RED `go test ./internal/config ./internal/queue` failed as expected before implementation; GREEN focused tests passed after implementation.
- Independent verifier observed `go test ./internal/config ./internal/queue` and `go test ./...` both pass. Writer also ran both suites successfully. The subsequent item 4 integration test was verified against live Compose Redis.
- `gentle_review assess` returned `unassessable` because intended untracked files were not declared; its fallback required an independent verifier, which passed.
- Work-unit commit for item 2: `4d70f42 feat(queue): add Redis Streams publisher`; medium native review approved and acknowledged after user authorized one reviewer run.
- Item 3 TDD evidence: `go test ./internal/api` failed as expected before the handler publisher interface existed, then passed after implementation and refactor check.
- Independent verifier observed `go test ./internal/api` and `go test ./...` pass, and confirmed CreateJob-before-Publish plus exact 503 response containing the created `job_id`.
- Work-unit commit for item 3: `0349424 feat(api): publish digest jobs to Redis`.
- Native review for `0349424` assessed medium and the user authorized one `review-reliability` run; capture approved and exact acknowledgement burned authority.
- `.env.example` direct read remained blocked. Per the user's explicit attestation and commit request, only that file was committed without exposing its content to the model; exact values were supplied by the user.

- Item 4 verification: `go test ./internal/queue`, `REDIS_URL=redis://localhost:6379/0 go test -tags integration ./internal/queue`, and `go test ./...` passed; integration test ran against Compose Redis, which was left running.
- Work-unit commit for item 4: `4fc88da test(queue): cover Redis Streams integration`; writer and independent verifier passed unit, tagged live Redis integration, and full suite.
- Native review for `4fc88da` assessed medium; after user authorized the one-run forecast, `review-reliability` approved and exact acknowledgement burned authority.

## Next step
Feature implementation is complete. Native review of the exact unreviewed candidate `d29c8ea7d1bfcc1e21e2504ef4c5fadb1be6a3bdab91bd5a5819377b3d247951` approved and was acknowledged. Full `go test ./...` passed after the env-sample commit. Direct `.env.example` contents remain unverified by the agent due the sensitive-path policy; the commit follows the user's explicit attestation.