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
| WS-06 | done | Claim-gated job processing and synchronous output contract | `internal/worker/worker.go`, `internal/worker/receiver.go`, `internal/worker/worker_test.go` | Observed RED/GREEN; `go test ./internal/worker`; invalid payload/DB mismatch/claim outcomes, terminal-only ACK, diagnosis and complete output |
| WS-07 | done | Fixed pool, Redis backpressure and two-stage shutdown | `internal/worker/pool.go`, `internal/worker/pool_test.go` | Observed RED/GREEN; `go test -race ./internal/worker`; channel-coordinated N limit, no extra reads, cancelable backoff, NOGROUP fatal and resource-safe drain |
| WS-08 | in_progress | Testable CLI startup and signal wiring | `cmd/worker/main.go`, `cmd/worker/main_test.go`, `README.md` | Observed RED/GREEN for deterministic startup boundaries; `go test ./cmd/worker`, `go build ./cmd/api ./cmd/worker`; initialization failure cleanup, no sensitive logging |
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
- Running committed authored line count: 6,310 (`490644e`: 356; `7c34423`: 342; `8ad7307`: 458; `8effcf2`: 771; `5caf33e`: 820; `a758cb7`: 1,587; `d4835f0`: 337; `c4b113b`: 1,057; `69b855d`: 582). Closed slices: approval/tracking; DB claims; configuration; consumer; network policy; HTTP/extraction; per-call statistics; processing; pool/shutdown. Next source review base: `69b855d5a67724b4af7f4a21548789a2b185157b`. Remaining slices: CLI; integration. Original 2,500–3,500 total forecast was exceeded by coherent security/regression coverage; keep the selected feature-branch-chain shape, never one accumulated branch review.
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

### WS-06 — complete

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

### WS-06 verified implementation, correction and native closure

- Writer `mutfhbec-c-9xvh` observed missing processor/result API compile RED then GREEN; initial source953lines (383worker/33receiver/537tests). Tracked diff-check could not prove untracked whitespace; independent checks covered all three files.
- First verifier `mutg7ttp-d-o227` passed unit/race/all/build/gofmt/whitespace/closed-source checks but repeated concurrency failed at worker_test.go310: scrapes1 outputs1 acks[] claims2. One winner does not imply one claim attempt. No production double scrape/output/ACK occurred.
- Tests-only writer `mutghyrv-e-t3te` used an outside-lock afterGetSnapshot barrier for both initial pending snapshots; loser third requery bypasses it. Old one-attempt assertion observed deterministic RED, then strengthened exact2attempts/3Gets/oneSucceeded+oneProcessingDuplicate/oneArticleUUIDresult/zeroACK+diagnosis/finalprocessing GREEN. All waits bounded, joined goroutines, no scheduler sleeps or mutex-held peer waits. Production worker.go/receiver.go hashes unchanged.
- Final verifier `mutgou3i-f-w6u9`: focused50/race50, unit0.003s/race1.013s, all/build/gofmt/all3untracked-whitespace/source quiet PASS; original concurrency/context race subset3x and full worker race10x (230 executions) PASS. Zero final findings/failures/skips/pending; old failure remains historical. Final source1011lines (383/33/595).
- Commit `c4b113bf6c882058595cdd334496b964d33d5879`, `feat(worker): process claimed jobs through a safe output contract`: 1048 additions/9 deletions1057authored; featuretotal5728. Native medium reliability `review-fb4d5591df0910c8`, exactbase `d4835f0be0fee0a9ef2558ee0831a6ec62ce92ec`, committedOnly/excludeduntracked: host-relay approved, fresh bound STATUS supplied exact acknowledgement, returned native-approved-acknowledgement-completed/authority burned/mutation committed. No STATUS after burn, no source advisory reopened. Real pool/CLI/integration proof was not claimed by this fake-based unit.
- Detailed original chronology remains in the committed task ledger; this compact closed milestone preserves failure, correction, verification, commit and native identities.

### WS-07 — complete

Map the existing Consumer.Read cancellation/finite budgets/NOGROUP contracts, Processor.Process admission-versus-job contexts, configured pool/drain/backoff settings, and required fixed-N/no-read-ahead/two-phase shutdown tests before a single pool writer. Approved source surfaces remain pool.go/pool_test.go/worker.go; prefer no changes to closed Processor behavior unless a concrete integration seam requires them. No CLI/resource bootstrap/service operations in this unit. Stop admission first, let confirmed jobs drain under independent job context, cancel jobs after drain budget and wait readers/jobs before caller closes resources. Do not invent hard exit within drain when readers have longer finite budgets.

### WS-07 map resolved — two-file writer scope

Explorer `mutgzvgq-g-r8oa` confirms Consumer.Read and Processor.Process fit minimal QueueReader/JobProcessor interfaces without worker.go edits. PoolOptions size4(1–64),ShutdownTimeout10s,BackoffDelay fixed1s per PLAN (testoverride, no new envvar),optional slog.Logger; NewPool/Run accept injected reader/processor, caller owns resourceClose. Use real Go context.WithoutCancel(root)+WithCancel forjobctx independentofadmission while preservingvalues; scout's illustrative WithoutDeadline is not a Go API. Parent Consumer50–89 spotcheck confirms exported GroupMissingError and errors.As IsGroupMissing.

ExactlyNpersistentworkers sequentialRead->Process,emptyBLOCKnormalcontinue,transientreadcancelablebackoff,NOGROUPcanceladmissionfatal/no recreate,perjoberrorcontinue safely/no businessretry. Rootstop closesadmission;confirmedjobsdrain10s concurrentwithreaderwait;expiredwindowcanceljobctx;waitALLreaders/jobs beforeRunreturn/callerClose. Longerfinite readbudgets mayextendexitbeyonddrain; no hardexit/read<=drain constraint/abandonedgoroutines. Deliveriesreturnedafterknownstop mustnotstartclaim/ACK. Deterministicchannel/barrier/atomic/timer-coordinatedtests,neverwaitwhilefakemutexheld/no sleeps/goroutinecount assertions. Scope narrowed to2newpoolfiles; no unresolvedproductdecision/sourceblocker.

### WS-07 writer evidence — independently checking

Writer `muthbjkz-h-c691` COMPLETE:2newfiles548lines(176pool/372tests),compile RED before pool API,unitGREEN/race/all/build/gofmt/both-new-file whitespace/closed-source quiet PASS. Fatal coverage expansion initially failed a test assuming both readers start before cancellation; corrected assumption, final unit PASS. Four selected pool/context/concurrency tests race10x PASS. No hidden production rewrite or real infrastructure readiness claimed.

Parent45–176 boundedspot confirms run-once guard,WithoutCancel values,constant root/drain watchers,Nsequential workers,safeNOGROUPUnwrap,cancelablebackoff and timer whilejoiningworkers. ASSESSunassessableunselecteduntracked/RDDonunknown/runtimeLarge requires independenthighfallback. Launched `gentle-ai-verify` `muthnh4d-i-j38y` to audit actual source/timer/cleanup/fatal late races and rerun unit/race/all/build/gofmt/newfilewhitespace/closedsourcequiet plus ALLpoolrace20x, not only four selected tests. No poolcommit/native transaction yet.

### WS-07 final independent verification — PASS

Verifier `muthnh4d-i-j38y` COMPLETE/zero open findings/failed/skipped/pending checks. Actualunit0.024s/race1.034s/all/build/gofmt/BOTHnewfilewhitespace/parentdocwhitespace/closedsourcequiet PASS; ALLpoolrace20x PASS1.433s. FixedN/noahead/slotreuse,lateDeliverygate,WithoutCancelvalues,NOGROUPsafeUnwrap,singlerun/pre-canceled0reads,concurrentdrainjobcancelwhilelongReaderblocked andallwatcher/workerjoins confirmed. Fatalreadcount1–2validrace-to-stop scheduling; independentlongreader/drain proof remains deterministic. No production/source corrections required. Fakesonly: CLI/liveintegrationstillpending.

### WS-07 work-unit and native closure — complete

Commit `69b855d5a67724b4af7f4a21548789a2b185157b`, `feat(worker): add fixed pool with two-stage shutdown`:2poolfiles+taskdoc,578 additions/4 deletions582 authored;featuretotal6310. Source548lines176/372. Guardedbranch/base/index/stagedwhitespacePASS; .codegraphonlyuntracked/no dotenvaccessstagingpublishing.
Native mediumreliability `review-164f6eb2a48c7e8b` exactbasec4b113bf6c882058595cdd334496b964d33d5879 committedOnly/excludeduntracked: singlepi_host_relayforecast/run approvedlast-eventclosure, freshboundSTATUSexactACK executedreturnednative-approved-acknowledgement-completed/authorityburned/mutationcommitted. No STATUS afterburn/no correctionopened. FinalindependentchecksabovePASS;CLI/liveintegrationstillpending.

### WS-08 — in progress; bounded read-only startup map

Map actual DB/Consumer/Scraper/Processor/Pool factories and close ownership, signal/init-error boundaries, safe default output receiver and README conventions before a single CLI writer on main.go/main_test.go/README.md. Tests must inject fakes/config and avoid invoking real LoadWorker or worker at repo root (existing loader can touch forbidden dotenv paths); no services/binaryruns/signals tohost. Keep two-phase semantics/terminal-onlyACK unchanged; startup failure closes every successfully acquired handle, Runreturn precedesClose, signalhandlers restored. No new API/config/schema/AI/persistence behavior.

### WS-08 contract map resolved

Explorer `muthw42y-j-6nty` resolves factory signatures/options/resourceclose/defaultreceiver andfake-onlytests without internalpackage changes. Parent PLAN135–159 spot confirms explicit synchronousmetadata-only receiver (noTextlogging/storage/anotherStream/persistence); deliverynondurableand processingunacked remains honest. Scout's illustrative text_length/title/languagefieldlist is not an exact mandatedschema: use safe lengths/presencebooleans orallowlists, never arbitraryarticleattributes.

Register SIGINT/SIGTERM context BEFORE opening connections, covering startup cancellation unlike scout's illustrative late-registration diagram. Restore registration onallreturns. Fake-injectedconfigloader/factories/NotifyContext testsnevercall realLoadWorker/worker main/rootdotenv/service/signal. ProductionhumanCLIusesunchangedLoadWorker. Bound DBopen/Ping andgroupinit with root+appropriatebudgets; acquiredhandlecleanupreverseconsumer->DB afterPoolRunjoined orinitfailure. MapALLvalidatedConsumer/Scraper/Processor/Poolsettings/backoff1s(noextraenvvar); safeerrorcodes withIsAsnotrawcredentials. Constructor/canceledroot/factory/run/cleanupfailuretests andmetadatareceivernopersistence/secretlogs; READMEhumancommandsnotexecuted/status/limits/no-recovery/drain-not-hard-exit. No blockers/productdecision.

### WS-08 interrupted writer — partial recovery

Writer `muti5obd-k-8po5` FAILED (generic assistant-reported error,14turns24calls); no completed verification/TDD handoff exists, so no inferred PASS or historical RED. HEAD69b855d unchanged; partial main.go175/main_test.go154=329lines, README unchanged. Parent metadata confirmed only allowed CLI paths plus own taskdelta/unrelated .codegraph; full mirror readback matched.

Bounded source read finds unfinished compile wiring: missing signal import, os.Exit called with an error instead of exit status, newProcessor factory db.DBTX/worker.Repo mismatch. These are source findings, NOT observed failing command evidence. Preserve partial files; resumed writer must recover any actual earlier command evidence from its own history and observe current RED before completing behavior, then finish missing tests/docs/checks. Harness failure cause remains unavailable beyond generic error; no tooling/env workaround assumed or authorized.

### WS-08 recovered writer — independent verification pending

Writer `mutxe24l-1-6n7a` completed ONLY main.go291/main_test.go550/README169. Current CLI source841lines; tracked CLI/README delta341 additions/12 deletions plus550new test lines. HEAD69b855d unchanged, parent task delta and .codegraph preserved. Historical interrupted writer error remains recorded, not inferred away.

Writer recovered initial missing injected-API/entry compile RED from its own session; current recovery compile RED exposed exit-wrapper type mismatch, missing signal import and DBTX/worker.Repo mismatch. Focused typed-nil factory test observed cleanup panic RED before normalization. Then GREEN/refactor, acquired-handle+error/typed-nil/close-order/cleanup-error/cancellation/NOGROUP-cause/mapping/safe receiver coverage. README has no meaningful RED; structural verification applies.

Writer reports unit/race/all/build/gofmt/all CLI race10x/tracked and actual-untracked whitespace/closed internal-modules-PLAN quiet PASS. Tests use fake loader/factories/notifier; no real loader/main/binary/services/signals/dotenv invocation. These are writer checks, not independent or live E2E evidence.

Parent bounded source1–216 spot-check confirms early registration/restoration, corrected Repo adapter, nil-normalized acquired handles and reverse cleanup after Run. ASSESS is unassessable from untracked scope, RDDon/native unknown/runtime large; high-risk independent verification required. One open audit point: newDB receives root directly while Ping receives DBTimeout; verify the explicit bounded DB-open startup requirement and coverage rather than assuming factory internals make it safe.

### WS-08 final independent verification — PASS

Re-verifier `mutyksj7-5-el1r` COMPLETE confirms prior DB-open budget BLOCKED finding RESOLVED. main.go144–146 bounds/cancels construction;157–159 root guard skips Ping after startup cancellation;160–163 Ping has an independent timeout. Value/earlier-deadline propagation, factory error cause and acquired-handle cleanup verified. No remaining observed defects, regressions, failed/skipped/pending CLI checks.

Actual unit0.028s/race1.035s/all/build/full CLI race10x1.277s/gofmt/tracked+actual-untracked whitespace/closed-source quiet PASS. CLI source957lines296/661, README hash unchanged2d1a852c23779542d782f20eeeb4a2a21cacad30ab8f1de715860ded1cc1b4a0. Fake-only verification; real end-to-end integration remains WS-09. Source/tests/docs form one coherent startup/cleanup area, exceeding the advisory400-line heuristic without minification or a file-type split.

Parent commit guard evidence: branchfeat/worker-scraper, HEAD69b855d, empty index, only3CLI paths+parenttaskdelta/.codegraphuntracked. main.go SHA256cfafd07af101e560c2c1b98df36b6e32f6b30d31c0ce4f9d74a4e5b059a01ca9; main_test.go f3be24670ae3b6827831f35592176b4653ec4c112d955182a3cb331e0256f1d3. No writer active. Ready for coherent CLI commit and native slice review; WS-08 stays in-progress until that closure.

## Next step

Independent verifier `muty379n-3-gn3s` returned BLOCKED: main.go:146 calls newDB with unbounded root rather than DBTimeout context; tests104/145 inspect only Ping/EnsureGroup budgets. Existing startup contract explicitly bounds DB-open and Ping. Actual unit0.002s/race1.009s/all/build/full CLI race10x1.019s/gofmt/tracked+actual-untracked whitespace/closed-source quiet PASS; no other blocking findings. Passing old tests does not close this assertion gap.

Bounded correction scope is ONLY cmd/worker/main.go and cmd/worker/main_test.go; README and all closed packages unchanged. Add a deadline regression and observe actual RED before source edits, then WithTimeout(root, cfg.DBTimeout) around newDB with cancellation immediately after return. Preserve returned-handle/error/typed-nil cleanup and an independent Ping budget (do not reuse canceled open context). Verify context values/root cancellation/earlier root deadline and no later services after open timeout, with bounded fake-only tests and no sleeps. Re-run unit/race/all/build/gofmt/full CLI race10x/whitespace/closed-source checks; continue independent verifier after corrected writer returns. No commit/native authority until blocker resolution is observed.

Correction writer `mutyalqz-4-i22w` COMPLETE, ONLY main.go/main_test.go: +5 source/+111 test lines, final296/661=957CLI source lines. README SHA256 unchanged `2d1a852c23779542d782f20eeeb4a2a21cacad30ab8f1de715860ded1cc1b4a0`; HEAD69b855d/allowed scope preserved.

Observed regression RED: TestDatabaseOpenGetsBoundedValueContextCanceledBeforeIndependentPing reports no finite DB-open deadline and open context not canceled after return; bounded context+immediate cancel GREEN. TestRootCanceledDuringDatabaseOpenSkipsPingAndLaterFactories then observed events[db-ping db-close] RED; immediate root cancellation guard before Ping GREEN. Earlier root deadline/value inheritance, independent live Ping context subsequently canceled, blocking fake open DeadlineExceeded cause and exactly-once returned-handle cleanup triangulated. No sleep/unbounded fake wait.

Writer unit/race/all/build/fullCLI race10/gofmt/tracked+actualuntracked whitespace/closed-source quiet PASS. No protected paths, loader/main/binary/service/signal calls. Parent metadata/hash and bounded source spot confirms root guard/independent Ping; ASSESS again unassessable untracked/RDDonunknown/runtimeLarge requires independent high-risk re-verification. Continued prior independent verifier as `mutyksj7-5-el1r` over the corrected candidate; original BLOCKED remains historical until its outcome. No writer runs concurrently. Wait for pushed completion, no polling. No commit/native authority yet.

WS-09 read-only map `mutxmp29-2-w8cw` is complete: PLAN.md:255 explicitly calls real worker/DB/Redis plus scraper fake. Scraper injection is package-private; do not expand APIs or pretend integration exercises concrete HTTP. Preserve separate deterministic scraper fetch proof. Three future paths suffice (worker_integration_test.go/README.md/Makefile); packages run `-p 1` to avoid test-DB lifecycle races. Guard only inteldigest_test and owned Redis keys, no application migrate prerequisites or FLUSH. Cases: duplicate exclusion/one scrape-result/processing-unacked, safe failure diagnosis, restart without PEL recovery while new entries progress, terminal-only ACK. Pool joins; caller closes resources. WS-09 remains pending and no integration writer is launched.
