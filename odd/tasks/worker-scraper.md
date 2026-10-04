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
| WS-00 | done | Record approved plan and implementation units | `PLAN.md`, `odd/tasks/worker-scraper.md` | Structural readback, `git diff --check`; documentation has no meaningful RED |
| WS-01 | done | Atomic processing claim and safe intermediate diagnostic | `internal/db/repository.go`, `internal/db/repository_unit_test.go`, `internal/db/repository_test.go` | Observed RED/GREEN; `go test ./internal/db`, race and real PostgreSQL claim race/terminal protection |
| WS-01V | done | Verify real DB concurrency and resolve integration blocker | No source edits; local PostgreSQL/test DB only after explicit authorization | Authorized test-DB reset, real integration PASS, independent unit/race checks and SQL review; no application DB/volume deletion |
| WS-02 | done | Worker-only validated configuration with human-managed examples | `internal/config/config.go`, `internal/config/config_test.go` only | Observed RED/GREEN; `go test ./internal/config`; API Load compatibility, overflow/duration/name validation; human dotenv attestation tracked in WS-02E |
| WS-02E | done | Human-managed configuration examples and confirmation | No agent dotenv access; proposed `.env.example` entries in chat only | Human preserves existing examples, applies new entries and explicitly confirms; record attestation, no invented agent readback |
| WS-03 | done | Bounded Redis consumer operations | `internal/queue/consumer.go`, `internal/queue/consumer_test.go`, `internal/queue/consumer_integration_test.go` | Observed RED/GREEN; `go test ./internal/queue`; exact group/read/ACK semantics, cancellation and real Redis PEL |
| WS-03R | done | Resolve local Redis integration readiness blocker | No source edits; local Redis and own random test keys after authorization | Explicit local service-start decision, no FLUSH/reset/unrelated key deletion, non-skipped integration and race PASS |
| WS-04 | done | Public URL/IP policy and DNS-safe dialing | `internal/scraper/types.go`, `internal/scraper/network.go`, `internal/scraper/network_test.go` | Observed RED/GREEN; `go test ./internal/scraper`; all special/private/metadata/mapped/ambiguous addresses, mixed DNS, literal dial pinning |
| WS-05 | done | Deadline-bounded HTTP fetch and useful HTML extraction | `go.mod`, `go.sum`, `internal/scraper/types.go`, `internal/scraper/network.go`, `internal/scraper/scraper.go`, `internal/scraper/scraper_test.go`, `internal/scraper/testdata/*` | Observed RED/GREEN; `go test ./internal/scraper`, race; redirects, cumulative deadline, body/gzip/MIME bounds and extraction; pinned compatible readability dependency |
| WS-05M | done | Per-call decompressed body statistics for approved observability | `internal/scraper/types.go`, `internal/scraper/scraper.go`, `internal/scraper/scraper_test.go` | Observed RED/GREEN; focused/race/regression, per-call gzip/charset byte accuracy, legacy Scrape and Article unchanged, no shared mutable stats |
| WS-06 | in_progress | Claim-gated job processing and synchronous output contract | `internal/worker/worker.go`, `internal/worker/receiver.go`, `internal/worker/worker_test.go` | Observed RED/GREEN; `go test ./internal/worker`; invalid payload/DB mismatch/claim outcomes, terminal-only ACK, diagnosis and complete output |
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
- Running committed authored line count: 4,334 (`490644e`: 356; `7c34423`: 342; `8ad7307`: 458; `8effcf2`: 771; `5caf33e`: 820; `a758cb7`: 1,587). Closed slices: approval/tracking (`0f7dab6..490644e`); DB claims (`490644e..7c34423`); configuration (`7c34423..8ad7307`); consumer (`8ad7307..8effcf2`); network policy (`8effcf2..5caf33e`); HTTP/extraction (`5caf33e..a758cb7`). Next source review base: `a758cb7928bcf81208df1651cab1ca6ef8c43332`. Remaining planned slices: processing; pool/shutdown; CLI; integration. Original 2,500–3,500 total forecast was exceeded by coherent security/regression coverage; preserve it without compression and keep the selected feature-branch-chain shape, never one accumulated branch review.
- Keep source, tests and relevant docs together; every task records commit ID, checks, runtime proof (or justified N/A), review tier/outcome and rollback boundary.

## Progress and evidence

Completed milestones are summarized below; their work-unit commits retain the original detailed chronology. This is a current recovery document, not an exhaustive command diary. Keep the entire document comfortably below the 50KB memory content bound and verify the full mirror, rather than accepting a truncated readback.

### WS-00 — complete

- Approved PLAN clarifications: scrape failure remains processing/unacked with diagnosis; one shared 30s request deadline across all hops, not 30s per redirect.
- Created feature branch from `0f7dab6724b7dad98542e48d6fab329b2370e0f3`; documentation commit `490644e4893ebbad69eb7bd8f96b192ba7daeeac`, `docs(worker): record approved scraper plan and work units` (298 additions/58 deletions).
- Independent verifier `mut1mun2-2-zpwa`: approved plan/unit mapping and doc whitespace/regression baseline PASS. Passive documentation has no meaningful RED; baseline tests are not source TDD evidence.
- Native docs lineage `review-5435fad14b4bdfdc` returned low/non_executable_only approved/action closed, no lenses, but STATUS then offered `empty_candidate_base_ref_required`. No acknowledged/consumed authority is claimed; no maintenance or lifecycle replay.

### WS-01 / WS-01V — complete

- Added atomic pending→processing conditional claim, error clearing, rows-affected gate and processing-only safe diagnosis capped at 1024 Unicode runes. No generic status/schema/API update.
- Observed compile RED for missing repository methods before implementation; GREEN/refactor unit/race/all/whitespace PASS, including zero/one-row, context/wrapped errors and ASCII/Unicode boundaries.
- User authorized local postgres startup and reset of ONLY `inteldigest_test`. Existing harness drops/recreates that test DB; application `inteldigest` and `pgdata` must never be reset/deleted. Service remains running.
- Verifier `mut4fepr-4-x1wv` COMPLETE/zero defects: pg_isready, unit/race, real integration (0.092s), sequential race integration (1.091s), all/whitespace PASS. Real committed-row race uses independent connections/barrier and proves exactly one winner; persisted processing diagnosis and terminal protection confirmed. No skipped/pending checks.
- Commit `7c344239a65c438e5dd1aae46f262f9b644843ae`, `feat(db): add atomic processing claims and scrape diagnostics` (337 additions/5 deletions).
- ASSESS unassessable from undeclared untracked files; independent verifier satisfies fallback. Native START initially rejected retained-selection/base mismatch without mutation. Fresh INSPECT and START with identical base/committedOnly resolved it.
- Native medium reliability lineage `review-8914e35d1ac4e0e9` approved; exact acknowledgement returned `native-approved-acknowledgement-completed`, authority burned/mutation committed. No STATUS after burn.

### WS-02 / WS-02E — complete

- WorkerConfig embeds Config; LoadWorker preserves API/worker validation isolation, defaults/limits, safe queue timeout arithmetic and body max+1 overflow guards.
- Sensitive-path read safeguard blocked initial example access before edits. Human chose NO agent access to ANY `.env`/`.env.*`, including examples: no read/write/stage/content diff/source/bypass. Human explicitly replied `aplicado` to proposed entries; attestation only, never automated readback. WORKER_CONSUMER_NAME intentionally omitted for autogeneration; existing connections remain human-owned.
- Observed compile RED for LoadWorker before production edits, then GREEN/refactor. Audit found defined-empty group/name incorrectly treated as absent. Correction `mut5cj32-7-racu` observed assertion RED on both explicit-empty settings, then GREEN using worker-only os.LookupEnv; truly unset fixtures/defaults and whitespace rejection preserved.
- Verifier `mut5igmm-8-yxav` COMPLETE/zero defects: explicit-empty targeted0.002s, unit0.003s/race1.010s, all/build/scoped whitespace PASS. Pure loader external integration N/A, not worker-runtime proof. No dotenv access/service operations or skipped required checks.
- Commit `8ad7307f805e953c6a102eccbd42fb4ee957e7b6`, `feat(config): add validated worker configuration` (427 additions/31 deletions), config.go/config_test.go/tracking only; no dotenv staging.
- Native medium reliability lineage `review-9971c5230e2fdcbd` approved; exact acknowledgement completed/burned/mutation committed; no STATUS after burn.

### WS-03 / WS-03R — complete

- COUNT1/finite BLOCK/`>`/no NOACK; typed Redis XReadGroup, Stream-ID ACK, group creation at0 MKSTREAM/exact BUSYGROUP, wrapped NOGROUP detection, retries off, idempotent Close and pool N+2. No XDEL/PEL recovery.
- Actual RESP3 integration exposed generic parsing error `unexpected streams result type map[interface {}]interface{}`; typed XReadGroup fixed it without forcing RESP2. Fractional-millisecond BLOCK is normalized at queue boundary. Cancellation test corrected to observe CLIENT LIST blocked-reader state before cancellation, not rely on scheduler sleep.
- Admission cancels immediately; an already blocked reader completes within finite bounds, normally BLOCK2s/worst block+operation7s. Job drain10s runs concurrently; wait readers/jobs before closing resources. Longer allowed read settings can extend exit beyond drain. No read≤drain constraint/hard process deadline; uncertain delivered PEL entries remain accepted non-recovery scope.
- User authorized local Redis startup; tests create/clean only unique owned keys. Never FLUSHDB/FLUSHALL/delete unrelated data; service remains running.
- Verifier `mut6vps9-d-dhfs` COMPLETE/zero defects: unit/race, both real distribution/ACK and blocked-cancellation/reuse scenarios, race integration, all/build/gofmt/whitespace PASS; no failed/skipped/pending checks.
- Commit `8effcf24f6446be28b4d9775fb5bbbfb96afd08f`, `feat(queue): add bounded Redis stream consumer` (771 authored lines).
- Native medium reliability lineage `review-502dd942dc30f615` approved; exact acknowledgement completed/burned. No STATUS after burn.

### WS-04 — complete

- Six-field Article and cause-preserving safe typed ScrapeError; absolute HTTP(S), no credentials/opaque/ambiguous hosts/zones, only scheme-standard ports, legacy numeric IP rejection, mapped IPv4 normalization, explicit nonpublic/special/metadata rules.
- Fresh resolution for each connection; vet the entire DNS set before any dial; dial only vetted literals under one DNS/connect budget. IPv6 conservative2000::/3 minus specials. No runtime address-list update or production private/TLS bypass.
- Observed tests-first RED/GREEN. Independent audit found Azure /16 overreach and blanket0x-domain rejection. Corrected with actual regression RED→GREEN: Azure exactly168.63.129.16/32; reject complete1–4-component numeric/hex IPv4-like grammar, allow legitimate0x.org/0xdeadbeef.example.com.
- Verifier `mutam5xb-4-mslb` COMPLETE: focused/race/all/build/gofmt/actual-untracked-whitespace PASS; zero open findings/failures/skips/pending. AS112 advisory not demonstrated SSRF threat, no unapproved expansion.
- 779 source/test lines; commit `5caf33e0c27ccb30c8d271bf7b5f69c6cc5c9848`, `feat(scraper): add public URL policy and pinned DNS dialing` (813 additions/7 deletions,820 authored).
- Native medium reliability lineage `review-ae452a78de1fa391` approved; exact acknowledgement completed/burned; no STATUS after burn.

### WS-05 — complete

- Dedicated HTTP transport, Proxy:nil/Jar:nil, no keep-alive/HTTP2; policy-approved literal dialing with original Host/TLS SNI and certificate checks preserved.
- Manual redirect loop validates every resolved destination and prevents HTTPS downgrade; default5/configured0 prohibits redirects. One shared30s deadline bounds all hops; connect/DNS5s/TLS5s/header10s/body-total10s capped by remaining time, body5MiB default.
- Status/MIME/charset/encoding validation, controlled gzip, shared decompressed max+1 response-body bound and original closer. No OS/TLS read-ahead cap is claimed. Charset expansion does not redefine the input size budget; finite bounded input feeds decoder/extractor.
- Dependency pins `codeberg.org/readeck/go-readability/v2 v2.1.3` (Go1.23/projectGo1.27.1), `golang.org/x/net v0.41.0`; pinned module API verified directly when Context7 unavailable. No FromURL/unrelated upgrades; uuid/pgx promoted direct without version change.
- Observed compile RED for missing API; unsupported-charset behavior RED→GREEN via charset.Lookup. Independent audit corroborated MIME buffering bypass and false secondary UTF8 cap. Correction writer `mutcortu-7-jd9c` observed regression RED, then GREEN: shared limiter before buffering; decoder streams already-bounded bytes directly to extraction without a second same-sized UTF8 cap.
- Verifier `mutd1d4j-8-11ir` COMPLETE:34 top-level tests/112 subtests, focused0.408s/race1.552s/all/build/gofmt/whitespace/pins PASS; deadline probes3x PASS1.128s. Zero failures/skips/pending/open findings.
- Source/test/fixtures1433 lines; commit `a758cb7928bcf81208df1651cab1ca6ef8c43332`, `feat(scraper): add deadline-bounded safe article fetching` (1577 additions/10 deletions,1587 authored). Committed feature authored total4334 before WS05M.
- INSPECT rejects START-only mode field with unknown-field/no mutation. Future INSPECT/START use SAME explicit baseRef/committedOnly and exclude unrelated untracked paths.
- Native medium reliability lineage `review-453b523a77d98590` approved; exact acknowledgement completed/burned/mutation committed; no STATUS after burn.
- Native R3-001 at scraper.go204–207 and R3-002 at311–318 were WARNING/informational/nonblocking. No descriptions supplied: do not invent meanings, reopen consumed candidate, automatically fix, or claim correction. Separately recorded follow-up only.
- Only local deterministic fake/httptest HTTP/TLS fixtures; no internet/DNS/article/services/private bypass/InsecureSkipVerify. Source fetch proof is not worker readiness.

### WS-06 — in progress; mapped and unblocked

- Approved writer surfaces remain `internal/worker/worker.go`, `internal/worker/receiver.go`, `internal/worker/worker_test.go`; no pool/CLI/DB/queue/schema/API changes in this unit. Outcome: validated versioned delivery, DB consistency, claim-gated contextual scraping, terminal-only ACK, safe intermediate failure diagnostic and synchronous in-memory result/error receptor. No terminal persistence/retries/AI.
- Bounded contract map is complete. Proceed with one strict-TDD worker-only writer: minimal consuming repository/acker/stats-scraper interfaces, synchronous Result/Receiver (JobID plus exactly one Article or typed ScrapeError), Process(admissionCtx,jobCtx,Delivery), injected DB/queue operation budgets and logger. Pre-claim DB work honors admission/job cancellation; confirmed claim winner continues on jobCtx even if admission closes. Output receiver honors jobCtx, is concurrency-safe, and never means terminal persistence. No helper/parser goroutines, pool or CLI in this unit. Caller must not confuse job UUID with Stream entry ID or scraping success with completion/ACK.
- Resolved observability gap: explorer confirms `len(body)` is discarded by current six-field Article/Scrape API; text bytes cannot stand in for downloaded bytes. Parent selected separate small WS-05M `ScrapeWithStats` additive API before orchestration, not mutable context observer/caches/deferred unknown metric. Future worker may consume the stats-bearing method while the legacy Scrape API and Article/receptor schema remain unchanged.
- Map `mutdhwa6-9-2rwz`: queue.Message has exactly `schema_version`, `job_id`, `url`, version1/UUID/syntactic HTTP(S) validation (no `job_type` field). Repository GetJobByID/TryMarkProcessing/RecordScrapeError, ErrNotFound, four JobStatus constants and Consumer Ack(ctx,Delivery) fit minimal consuming interfaces. Only proven completed/failed state ACKs exact Stream ID; claimed success/failure remains processing/unacked. Recommend Process(admissionCtx,jobCtx,Delivery); admission gating before SQL and bounded pre-claim contexts, post-claim job drain distinct. Typed local timeout may wrap Canceled and must not become operational cancellation; actual job-context cancellation is operational. Receiver sink errors never fabricate scrape diagnostics.

### WS-06 routing result

Explorer `mutdhwa6-9-2rwz` returned the bounded read-only map; no source writes or verification commands. WS-05M now closes the technical metric gap in already-approved PLAN8; consume its stats API without widening worker scope into scraper internals or inventing values. All real scrape failures remain processing/unacked; only actual job-context shutdown cancellation is operational. Real typed local timeout wrapping Canceled while jobCtx is live must still be diagnosed/output. Unknown/untyped causes and classification/status/URL/title/text/credential data must not leak through logs or diagnostic strings; preserve causes only for errors.Is/As. Receiver errors are independent sink errors. Safe classification helpers do not introduce business retries/terminal writes.

### WS-05M — complete

- Add `ScrapeStats{BytesRead int64}` and `ScrapeWithStats(ctx,rawURL) (Article,ScrapeStats,error)`; legacy `Scrape` delegates and discards stats. Preserve six Article fields and original error/cause/body/policy semantics; no module/worker/transport/logging changes or native-advisory repairs.
- BytesRead is exact decompressed FINAL response body byte count after a successful bounded body read, before charset conversion/extraction; zero until a complete body is available, retained on later failures. Not total TCP/TLS/redirect/UTF8-output/text bytes and not a promise of partial-failure byte counts. Use local per-call stats, never shared mutable pointer/context map/cache. Worker uses the known measurement on successful scrape; receiver/AI input unchanged.
- Strict tests-first compile/assertion RED, GREEN and refactor: legacy compatibility, body vs text vs Unicode and gzip distinction, pre-read failure zero/later extraction failure retains known count, concurrent calls independent, existing fetch/network/timebody regressions unchanged. Exact allowed source paths are the three scraper files in table. Parent owns PLAN/task/mirror/todo, commit and native review.

### WS-05M writer evidence — independently verified

Writer `mutdzuwj-a-rej8` returned COMPLETE: compile RED on missing Stats/method before implementation, GREEN and final triangulation/refactor/post-format focused PASS (0.408s), race PASS (1.558s), all/build/gofmt/scoped whitespace/modules unchanged PASS, no failed/skipped checks. Three allowed source files only163 additions/0 deletions (14 implementation,11 types,138 tests); parent docs and unrelated `.codegraph/` untouched. Tests cover gzip raw count versus compressed/text lengths, charset source count retained on expansion/extraction failure, redirects excluded/early failures zero/legacy compatibility and concurrent isolated calls. No native-advisory changes or worker-readiness claim.

Parent bounded source spotcheck confirmed local Stats and after-read/pre-decode capture. ASSESS unassessable from undeclared `.codegraph/` required high-risk independent fallback; verifier below fulfills it. Parent focused stats PASS0.006s/staged whitespace PASS. Doc grew49,971bytes and mirror readback caught truncation; condensed closed history into a verified ledger, retaining original histories in prior commits, all native/commit identities, TDD/corrections and constraints. Full current mirror357 readback then matched; doc24,867bytes before this closure. No partial mirror or source check failure.

Work-unit commit `d4835f0be0fee0a9ef2558ee0831a6ec62ce92ec`, `feat(scraper): expose per-call response byte statistics`: five explicitly staged paths (3Go+PLAN/tracking),229 additions/108 deletions337 authored; committed feature total4671. Unrelated `.codegraph/` remains untracked; no dotenv access/staging. Native medium reliability `review-04510dd7617df794` against exact base `a758cb7928bcf81208df1651cab1ca6ef8c43332`, committedOnly/excluded untracked: single pi_host_relay forecast/run approved at last event; fresh STATUS offered exact acknowledgement and it returned native-approved-acknowledgement-completed/authority burned/mutation committed. No STATUS after burn, no correction, no source advisories reopened.

Independent verifier `muteb3ta-b-l7t5` returned COMPLETE, zero open findings/failed/skipped/pending checks: focused0.410s/race1.577s/all/build/gofmt/scoped whitespace/unchanged modules-policy-fixtures PASS. Four focused stats/concurrency tests repeated three times PASS (0.007/0.007/0.006s) and race subset PASS (1.028s). Actual three-file163-addition implementation preserves Article6 and legacy APIs/causes; final decompressed bytes are exact, pre-charset/redirect-exclusive, zero before completed read and retained on later extraction failure; calls remain isolated without mutable shared stats.

### WS-06 writer evidence — independent verification pending

Writer `mutfhbec-c-9xvh` returned COMPLETE: actual compile RED for missing processor/result APIs before production implementation, focused GREEN and triangulation/refactor for concurrent same-job claim/cancellation/requery/local typed timeout/receiver errors/safe logging/bounded contexts. Unit/race/all/build/gofmt PASS. Three NEW untracked source files953 lines (383worker,33receiver,537tests); no other source/modules/PLAN changes. This is one orchestration area, not pool/CLI/integration readiness.

Whitespace evidence caveat: tracked diff-check saw no new worker files, and writer no-index checked only worker_test.go. Independent verification must check all THREE untracked files without staging, not infer clean whitespace from an empty tracked diff.

Parent bounded100–274 source spotcheck confirms preclaim DB budgets honor both parents, stored ID/URL checks before terminal-only ACK and lost-claim requery, confirmed winner uses jobCtx independent of admission, actual jobCtx completion controls operational cancellation, and receiver errors are separate from real scrape diagnosis. ASSESS remains unassessable from undeclared untracked files/RDDonunknown/runtimeLarge; high-risk independent fallback required. Launched `gentle-ai-verify` task `mutg7ttp-d-o227` for actual source/controlflow/coverage audit and unit/race/all/build/gofmt/all-new-file whitespace/existing-source quiet checks. No WS-06 commit or native transaction yet.

### WS-06 initial independent verification — test flake observed

Verifier `mutg7ttp-d-o227` returned BLOCKED: initial worker unit0.003s/race1.013s/all/build/gofmt/ALL THREE new-file no-index whitespace/parentdoc whitespace/existing-source quiet checks PASS, but focused concurrency/context race repetition3 FAILED at worker_test.go310: scrapes1 outputs1 acks[] claims2. The test incorrectly requires exactly one claim ATTEMPT; two concurrent initial pending snapshots legitimately produce two attempts with only one winner. No production double scrape/output/ACK was observed; candidate still fails required repeated checks and must not be committed/reviewed as complete.

Parent selected a tests-only deterministic correction: force both initial pending snapshots using an outside-lock barrier (existing testRepo.GetJobByID invokes onGet while holding mu; do NOT put a two-reader wait there), preserve the old one-attempt assertion to observe deterministic RED, then assert two attempts/exactly one successful result/one processing duplicate/one scrape/one output/zero ACK and no diagnostic/terminal writes. Lost-claim requery must not wait on the initial-read barrier. No production or broader source changes; strengthen winner proof rather than masking the flake by loosening every assertion. Bound barrier waits/cancellation and join goroutines, no scheduler sleeps.

### WS-06 deterministic test correction — independently rechecking

Correction writer `mutghyrv-e-t3te` returned COMPLETE, ONLYworker_test.go: afterGetSnapshot outside-lock fake hook, bounded first-two-snapshot barrier bypassed by loser requery, strengthened exact2attempts/3Gets/oneSuccess+oneProcessingDuplicate(nilerrors)/oneArticleUUIDresult/zeroACK+diagnosis/finalprocessing assertions. Actual deterministic old-one-attempt assertion RED: succeeded1 processingduplicates1 scrapes1 outputs1 acks[] claims2; corrected assertion GREEN. Focused50x/race50x/unit/race/all/build/gofmt/ALL3new-file noindexwhitespace/source-modulequiet PASS. Before/after worker.go and receiver.go hashes matched (no production edits). Current1011lines=383worker+33receiver+595tests.

Parent boundedtest300–404 spotcheck confirms boundedwait/join/results/strongerwinner assertions; ASSESSaftercorrection again unassessable undeclareduntracked/RDDonunknown/runtimeLarge -> high-risk independent verifier required. Continued prior verifier `mutg7ttp-d-o227` in fresh task `mutgou3i-f-w6u9` to independently inspect helper lock/snapshot ordering/requery bypass and repeat actual focused50/race50/unit/race/all/build/gofmt/all-untracked-whitespace/sourcequiet plus3xoriginalconcurrency/context race subset. Old failed check remains historical evidence, not a current completion claim. No WS06commit/native transaction yet.

### WS-06 final independent verification — PASS

Re-verifier `mutgou3i-f-w6u9` COMPLETE/zero open findings: actual focused50x0.005s/race50x1.023s,worker unit0.003s/race1.013s,all/build/gofmt/ALL3new-file noindexwhitespace/parentdocwhitespace/existing-source quiet PASS. Original concurrency/drain/localtimeout race subset3x PASS1.012/1.014/1.012s; entire worker race10x PASS1.048s (230 test executions). No failed/skipped/pending final checks. Old failing assertion remains historical, corrected through observed deterministic RED/GREEN rather than weakened production behavior.

Source audit confirms afterGetSnapshot strictly outside mutex, old onGet cancellation semantics retained, both pending snapshots before claims, loser third read bypass, exact2attempts/1Succeeded+1ProcessingDuplicate/1scrape+ArticleUUIDoutput/0ACK+diagnosis/finalprocessing. Production source hash identities unchanged; candidate1011lines. Only deterministic fakes; real infrastructure/pool/CLI remain future.

## Next step

Commit verified coherent WS-06 explicit3workerfiles+parenttracking, then native review against exact base `d4835f0be0fee0a9ef2558ee0831a6ec62ce92ec`, excluding .codegraph. No further functional verification loop absent new changes/findings. After native closure, begin bounded WS-07 pool/shutdown mapping. No dotenv access/service operations/publishing/new business scope.
