package worker

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"inteldigest/internal/db"
	"inteldigest/internal/models"
	"inteldigest/internal/queue"
	"inteldigest/internal/scraper"
)

type testRepo struct {
	mu                                         sync.Mutex
	job                                        *models.Job
	getErr                                     error
	onGet                                      func()
	afterGetSnapshot                           func()
	claim                                      bool
	claimErr                                   error
	claimLostStatus                            models.JobStatus
	claimCalls, getCalls, recordCalls          int
	getContexts, claimContexts, recordContexts []context.Context
	recordReason                               string
	recordErr                                  error
	recordOK                                   bool
}

func (r *testRepo) GetJobByID(ctx context.Context, _ uuid.UUID) (*models.Job, error) {
	r.mu.Lock()
	r.getCalls++
	r.getContexts = append(r.getContexts, ctx)
	if r.onGet != nil {
		r.onGet()
	}
	if r.getErr != nil {
		err := r.getErr
		r.mu.Unlock()
		return nil, err
	}
	if r.job == nil {
		r.mu.Unlock()
		return nil, db.ErrNotFound
	}
	cp := *r.job
	afterGetSnapshot := r.afterGetSnapshot
	r.mu.Unlock()
	if afterGetSnapshot != nil {
		afterGetSnapshot()
	}
	return &cp, nil
}
func (r *testRepo) TryMarkProcessing(ctx context.Context, _ uuid.UUID) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimCalls++
	r.claimContexts = append(r.claimContexts, ctx)
	won := r.claim
	if won {
		r.claim = false
		if r.job != nil {
			r.job.Status = models.StatusProcessing
		}
	} else if r.job != nil && r.claimLostStatus != "" {
		r.job.Status = r.claimLostStatus
	}
	return won, r.claimErr
}
func (r *testRepo) RecordScrapeError(ctx context.Context, _ uuid.UUID, s string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recordCalls++
	r.recordContexts = append(r.recordContexts, ctx)
	r.recordReason = s
	return r.recordOK, r.recordErr
}

type testAcker struct {
	mu       sync.Mutex
	ids      []string
	err      error
	contexts []context.Context
}

func (a *testAcker) Ack(ctx context.Context, d queue.Delivery) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ids = append(a.ids, d.ID)
	a.contexts = append(a.contexts, ctx)
	return a.err
}

type testScraper struct {
	mu       sync.Mutex
	article  scraper.Article
	stats    scraper.ScrapeStats
	err      error
	onScrape func()
	calls    int
	urls     []string
	ctx      context.Context
}

func (s *testScraper) ScrapeWithStats(ctx context.Context, u string) (scraper.Article, scraper.ScrapeStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.onScrape != nil {
		s.onScrape()
	}
	s.urls = append(s.urls, u)
	s.ctx = ctx
	return s.article, s.stats, s.err
}

type testReceiver struct {
	mu      sync.Mutex
	results []Result
	err     error
}

func (r *testReceiver) Receive(_ context.Context, v Result) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, v)
	return r.err
}
func testDelivery(id uuid.UUID, streamID string, raw any) queue.Delivery {
	b, _ := json.Marshal(raw)
	return queue.Delivery{ID: streamID, Fields: map[string]interface{}{"data": string(b)}}
}
func payload(id uuid.UUID, url string) map[string]any {
	return map[string]any{"schema_version": 1, "job_id": id.String(), "url": url}
}
func makeProcessor(t *testing.T, r *testRepo, a *testAcker, s *testScraper, o *testReceiver) *Processor {
	t.Helper()
	p, err := NewProcessor(r, a, s, o, Options{DBTimeout: time.Second, QueueOperationTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func testJob(id uuid.UUID, url string, status models.JobStatus) *models.Job {
	return &models.Job{ID: id, URL: url, Status: status}
}

func TestProcessRejectsInvalidPayloadWithoutEffects(t *testing.T) {
	id := uuid.New()
	r := &testRepo{job: testJob(id, "https://example.test", models.StatusPending), claim: true}
	a := &testAcker{}
	s := &testScraper{}
	out := &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	cases := []struct {
		name     string
		delivery queue.Delivery
	}{{"missing data", queue.Delivery{ID: "1-0"}}, {"wrong field type", queue.Delivery{ID: "2-0", Fields: map[string]interface{}{"data": []byte(`{}`)}}}, {"malformed", queue.Delivery{ID: "3-0", Fields: map[string]interface{}{"data": "{"}}}, {"non-object", queue.Delivery{ID: "3-1", Fields: map[string]interface{}{"data": "[]"}}}, {"unknown version", testDelivery(id, "4-0", map[string]any{"schema_version": 99, "job_id": id.String(), "url": "https://example.test"})}, {"invalid UUID", testDelivery(id, "5-0", map[string]any{"schema_version": 1, "job_id": "nope", "url": "https://example.test"})}, {"invalid URL", testDelivery(id, "6-0", payload(id, "file:///secret?token=private"))}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := p.Process(context.Background(), context.Background(), tc.delivery)
			if err == nil || got != OutcomeSkipped {
				t.Fatalf("Process=%v,%v; want skipped error", got, err)
			}
			if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unsafe error: %v", err)
			}
		})
	}
	if r.getCalls != 0 || r.claimCalls != 0 || s.calls != 0 || len(out.results) != 0 || len(a.ids) != 0 {
		t.Fatalf("invalid payload effects: get=%d claim=%d scrape=%d output=%d ack=%d", r.getCalls, r.claimCalls, s.calls, len(out.results), len(a.ids))
	}
}
func TestProcessClaimWinnerScrapesAndDeliversWithoutAck(t *testing.T) {
	id := uuid.New()
	article := scraper.Article{Text: "text", RequestedURL: "https://persisted.test", FinalURL: "https://final.test", Title: "title"}
	r := &testRepo{job: testJob(id, article.RequestedURL, models.StatusPending), claim: true}
	a := &testAcker{}
	s := &testScraper{article: article, stats: scraper.ScrapeStats{BytesRead: 91}}
	out := &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "171000-2", payload(id, article.RequestedURL)))
	if err != nil || got != OutcomeSucceeded {
		t.Fatalf("Process=%v,%v", got, err)
	}
	if s.calls != 1 || s.urls[0] != article.RequestedURL || len(out.results) != 1 || len(a.ids) != 0 {
		t.Fatalf("scrape=%d urls=%v output=%d ack=%v", s.calls, s.urls, len(out.results), a.ids)
	}
	res := out.results[0]
	if res.JobID != id || res.Article == nil || *res.Article != article || res.Error != nil {
		t.Fatalf("result=%+v", res)
	}
}
func TestProcessTerminalDuplicateAcksOriginalStreamID(t *testing.T) {
	id := uuid.New()
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusCompleted)}
	a := &testAcker{}
	s := &testScraper{}
	out := &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "171000-9", payload(id, "https://db.test")))
	if err != nil || got != OutcomeTerminalDuplicate {
		t.Fatalf("Process=%v,%v", got, err)
	}
	if len(a.ids) != 1 || a.ids[0] != "171000-9" || s.calls != 0 || len(out.results) != 0 || r.claimCalls != 0 {
		t.Fatalf("ack=%v scrape=%d output=%d claims=%d", a.ids, s.calls, len(out.results), r.claimCalls)
	}
	if len(a.contexts) != 1 {
		t.Fatalf("ACK context count = %d", len(a.contexts))
	}
	if _, ok := a.contexts[0].Deadline(); !ok {
		t.Fatal("ACK context has no finite deadline")
	}
	if len(r.getContexts) != 1 {
		t.Fatalf("GET context count = %d", len(r.getContexts))
	}
	if _, ok := r.getContexts[0].Deadline(); !ok {
		t.Fatal("DB context has no finite deadline")
	}
}
func TestProcessProcessingDuplicateDoesNotAckOrScrape(t *testing.T) {
	id := uuid.New()
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusProcessing)}
	a := &testAcker{}
	s := &testScraper{}
	out := &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "4-0", payload(id, "https://db.test")))
	if err != nil || got != OutcomeProcessingDuplicate || len(a.ids) != 0 || s.calls != 0 || len(out.results) != 0 {
		t.Fatalf("result=%v err=%v ack=%v scrape=%d output=%d", got, err, a.ids, s.calls, len(out.results))
	}
}
func TestProcessClaimLossRequeriesAndOnlyAcksProvenTerminal(t *testing.T) {
	id := uuid.New()
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claimLostStatus: models.StatusFailed}
	a := &testAcker{}
	s := &testScraper{}
	out := &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "8-1", payload(id, "https://db.test")))
	if err != nil || got != OutcomeTerminalDuplicate || r.getCalls != 2 || len(a.ids) != 1 || a.ids[0] != "8-1" || s.calls != 0 {
		t.Fatalf("result=%v err=%v gets=%d acks=%v scrapes=%d", got, err, r.getCalls, a.ids, s.calls)
	}
}
func TestProcessScrapeFailureDiagnosesAndDeliversTypedError(t *testing.T) {
	id := uuid.New()
	cause := errors.New("dns secret.invalid?credential=x")
	scrapeErr := &scraper.ScrapeError{Kind: scraper.ScrapeErrorNetwork, Stage: scraper.ScrapeStageFetch, Message: "raw secret", Cause: cause}
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true, recordOK: true}
	a := &testAcker{}
	s := &testScraper{err: fmt.Errorf("wrapped: %w", scrapeErr)}
	out := &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "9-2", payload(id, "https://db.test")))
	if err != nil || got != OutcomeScrapeFailed || r.recordCalls != 1 || len(out.results) != 1 || out.results[0].Error == nil || out.results[0].Article != nil || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v recorded=%d outputs=%+v ack=%v", got, err, r.recordCalls, out.results, a.ids)
	}
	if strings.Contains(r.recordReason, "secret") || strings.Contains(errString(err), "secret") {
		t.Fatalf("unsafe diagnosis/error: %q %v", r.recordReason, err)
	}
	var delivered *scraper.ScrapeError
	if !errors.As(out.results[0].Error, &delivered) || !errors.Is(delivered, cause) {
		t.Fatalf("scrape cause was not preserved: %#v", out.results[0].Error)
	}
}
func errString(e error) string {
	if e == nil {
		return ""
	}
	return e.Error()
}
func TestProcessActualJobCancellationIsOperational(t *testing.T) {
	id := uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending)}
	a := &testAcker{}
	s := &testScraper{}
	out := &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), ctx, testDelivery(id, "10-1", payload(id, "https://db.test")))
	if err == nil || got != OutcomeOperationalCanceled || r.claimCalls != 0 || s.calls != 0 || r.recordCalls != 0 || len(out.results) != 0 || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v claim=%d scrape=%d diagnosis=%d output=%d ack=%d", got, err, r.claimCalls, s.calls, r.recordCalls, len(out.results), len(a.ids))
	}
}
func TestProcessConcurrentSameJobHasSingleWinner(t *testing.T) {
	id := uuid.New()
	barrierMu := sync.Mutex{}
	initialSnapshots := 0
	snapshotsReleased := make(chan struct{})
	afterGetSnapshot := func() {
		barrierMu.Lock()
		initialSnapshots++
		waitForPeers := initialSnapshots <= 2
		if initialSnapshots == 2 {
			close(snapshotsReleased)
		}
		barrierMu.Unlock()
		if !waitForPeers {
			return // The claim loser must be able to perform its requery.
		}
		select {
		case <-snapshotsReleased:
		case <-time.After(3 * time.Second):
			t.Error("timed out waiting for both initial pending snapshots")
		}
	}
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true, afterGetSnapshot: afterGetSnapshot}
	a := &testAcker{}
	s := &testScraper{}
	out := &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	type processResult struct {
		outcome Outcome
		err     error
	}
	results := make(chan processResult, 2)
	var wg sync.WaitGroup
	ready := make(chan struct{}, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			got, err := p.Process(context.Background(), context.Background(), testDelivery(id, fmt.Sprintf("12-%d", n), payload(id, "https://db.test")))
			results <- processResult{outcome: got, err: err}
		}(i)
	}
	<-ready
	<-ready
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for concurrent Process calls")
	}
	var succeeded, processingDuplicate int
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil {
			t.Errorf("Process returned error: %v", result.err)
		}
		switch result.outcome {
		case OutcomeSucceeded:
			succeeded++
		case OutcomeProcessingDuplicate:
			processingDuplicate++
		default:
			t.Errorf("unexpected Process outcome: %v", result.outcome)
		}
	}
	if succeeded != 1 || processingDuplicate != 1 || s.calls != 1 || len(out.results) != 1 || out.results[0].JobID != id || len(a.ids) != 0 || r.claimCalls != 2 || r.getCalls != 3 || r.recordCalls != 0 || r.job.Status != models.StatusProcessing {
		t.Fatalf("succeeded=%d processing duplicates=%d scrapes=%d outputs=%+v acks=%v gets=%d claims=%d diagnostics=%d status=%s", succeeded, processingDuplicate, s.calls, out.results, a.ids, r.getCalls, r.claimCalls, r.recordCalls, r.job.Status)
	}
}
func TestProcessAdmissionClosingDuringPreclaimLookupPreventsClaim(t *testing.T) {
	id := uuid.New()
	admission, closeAdmission := context.WithCancel(context.Background())
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), onGet: closeAdmission}
	a, s, out := &testAcker{}, &testScraper{}, &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(admission, context.Background(), testDelivery(id, "13-1", payload(id, "https://db.test")))
	if got != OutcomeSkipped || err == nil || r.claimCalls != 0 || s.calls != 0 || len(out.results) != 0 || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v claims=%d scrapes=%d outputs=%d acks=%d", got, err, r.claimCalls, s.calls, len(out.results), len(a.ids))
	}
}
func TestProcessUncertainClaimNeverScrapes(t *testing.T) {
	id := uuid.New()
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claimErr: errors.New("database secret")}
	a, s, out := &testAcker{}, &testScraper{}, &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "14-1", payload(id, "https://db.test")))
	if got != OutcomeError || err == nil || strings.Contains(err.Error(), "secret") || s.calls != 0 || len(out.results) != 0 || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v scrape=%d output=%d ack=%d", got, err, s.calls, len(out.results), len(a.ids))
	}
}
func TestProcessClaimLossPendingDoesNotAckOrScrape(t *testing.T) {
	id := uuid.New()
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending)}
	a, s, out := &testAcker{}, &testScraper{}, &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "15-1", payload(id, "https://db.test")))
	if got != OutcomeSkipped || err == nil || r.getCalls != 2 || s.calls != 0 || len(out.results) != 0 || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v gets=%d scrape=%d output=%d ack=%d", got, err, r.getCalls, s.calls, len(out.results), len(a.ids))
	}
}
func TestProcessLiveJobLocalTimeoutWrappingCanceledIsBusinessFailure(t *testing.T) {
	id := uuid.New()
	typed := &scraper.ScrapeError{Kind: scraper.ScrapeErrorTimeout, Stage: scraper.ScrapeStageFetch, Cause: context.Canceled}
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true, recordOK: true}
	a, s, out := &testAcker{}, &testScraper{err: typed}, &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "16-1", payload(id, "https://db.test")))
	if got != OutcomeScrapeFailed || err != nil || r.recordCalls != 1 || len(out.results) != 1 || out.results[0].Error == nil || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v diagnoses=%d outputs=%d acks=%d", got, err, r.recordCalls, len(out.results), len(a.ids))
	}
}
func TestProcessConfirmedWinnerDrainsAfterAdmissionCloses(t *testing.T) {
	id := uuid.New()
	admission, closeAdmission := context.WithCancel(context.Background())
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true}
	a, out := &testAcker{}, &testReceiver{}
	s := &testScraper{article: scraper.Article{Text: "drained"}, onScrape: closeAdmission}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(admission, context.Background(), testDelivery(id, "16-2", payload(id, "https://db.test")))
	if got != OutcomeSucceeded || err != nil || s.calls != 1 || len(out.results) != 1 || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v scrape=%d outputs=%d acks=%d", got, err, s.calls, len(out.results), len(a.ids))
	}
}

func TestProcessJobCancellationDuringScrapeSkipsDiagnosisAndOutput(t *testing.T) {
	id := uuid.New()
	jobCtx, cancel := context.WithCancel(context.Background())
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true}
	a, out := &testAcker{}, &testReceiver{}
	s := &testScraper{err: &scraper.ScrapeError{Kind: scraper.ScrapeErrorNetwork, Stage: scraper.ScrapeStageFetch}, onScrape: cancel}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), jobCtx, testDelivery(id, "16-3", payload(id, "https://db.test")))
	if got != OutcomeOperationalCanceled || err == nil || r.recordCalls != 0 || len(out.results) != 0 || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v diagnosis=%d outputs=%d acks=%d", got, err, r.recordCalls, len(out.results), len(a.ids))
	}
}

func TestScrapeClassificationUsesAllowlistedCodesAndCauses(t *testing.T) {
	for _, tc := range []struct {
		status int
		want   string
	}{{404, "permanent"}, {408, "transient"}, {429, "transient"}, {503, "transient"}} {
		e := &scraper.ScrapeError{Kind: scraper.ScrapeErrorHTTPStatus, HTTPStatus: tc.status}
		if got := classify(e, nil); got != tc.want {
			t.Errorf("HTTP %d class = %q, want %q", tc.status, got, tc.want)
		}
	}
	certErr := x509.CertificateInvalidError{Reason: x509.Expired}
	if got := classify(&scraper.ScrapeError{Kind: scraper.ScrapeErrorNetwork}, certErr); got != "permanent" {
		t.Errorf("certificate class = %q", got)
	}
	if got := classify(&scraper.ScrapeError{Kind: scraper.ScrapeErrorTimeout}, context.DeadlineExceeded); got != "transient" {
		t.Errorf("timeout class = %q", got)
	}
	if got := classify(&scraper.ScrapeError{Kind: scraper.ScrapeErrorCanceled}, context.Canceled); got != "unknown" {
		t.Errorf("canceled-with-live-job class = %q", got)
	}
}

func TestProcessReceiverFailureNeverCreatesSyntheticDiagnosis(t *testing.T) {
	id := uuid.New()
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true}
	a, s, out := &testAcker{}, &testScraper{article: scraper.Article{Text: "body"}}, &testReceiver{err: errors.New("sink secret")}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "17-1", payload(id, "https://db.test")))
	if got != OutcomeError || err == nil || strings.Contains(err.Error(), "secret") || r.recordCalls != 0 || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v diagnoses=%d acks=%d", got, err, r.recordCalls, len(a.ids))
	}
}
func TestProcessScrapeErrorReceiverFailureKeepsOnlyRealDiagnosis(t *testing.T) {
	id := uuid.New()
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true, recordOK: true}
	a, s, out := &testAcker{}, &testScraper{err: &scraper.ScrapeError{Kind: scraper.ScrapeErrorBodyTooLarge, Stage: scraper.ScrapeStageFetch}}, &testReceiver{err: errors.New("sink secret")}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "18-1", payload(id, "https://db.test")))
	if got != OutcomeError || err == nil || strings.Contains(err.Error(), "secret") || r.recordCalls != 1 || r.recordReason != "scraping/body_too_large during fetch" || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v diagnoses=%d reason=%q acks=%d", got, err, r.recordCalls, r.recordReason, len(a.ids))
	}
}
func TestProcessDiagnosisPersistenceFailureStillDeliversRealScrapeError(t *testing.T) {
	id := uuid.New()
	var logs strings.Builder
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true, recordErr: errors.New("database cause secret")}
	a, s, out := &testAcker{}, &testScraper{err: &scraper.ScrapeError{Kind: scraper.ScrapeErrorNetwork, Stage: scraper.ScrapeStageFetch}}, &testReceiver{}
	p, err := NewProcessor(r, a, s, out, Options{DBTimeout: time.Second, QueueOperationTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "18-2", payload(id, "https://db.test")))
	if got != OutcomeScrapeFailed || err != nil || len(out.results) != 1 || out.results[0].Error == nil || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v outputs=%d acks=%d", got, err, len(out.results), len(a.ids))
	}
	if !strings.Contains(logs.String(), "scrape_diagnostic_persistence_problem") || strings.Contains(logs.String(), "secret") {
		t.Fatalf("persistence status missing or unsafe: %s", logs.String())
	}
}

func TestReceiverResultRequiresValidIDAndExactlyOnePayload(t *testing.T) {
	article := &scraper.Article{Text: "body"}
	scrapeErr := &scraper.ScrapeError{Kind: scraper.ScrapeErrorNetwork, Stage: scraper.ScrapeStageFetch}
	for _, tc := range []struct {
		name   string
		result Result
		valid  bool
	}{{"missing id", Result{Article: article}, false}, {"neither", Result{JobID: uuid.New()}, false}, {"both", Result{JobID: uuid.New(), Article: article, Error: scrapeErr}, false}, {"article", Result{JobID: uuid.New(), Article: article}, true}, {"error", Result{JobID: uuid.New(), Error: scrapeErr}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateResult(tc.result)
			if (err == nil) != tc.valid {
				t.Fatalf("validateResult err=%v valid=%v", err, tc.valid)
			}
		})
	}
}

func TestProcessRejectsStoredIdentityURLAndUnknownStatus(t *testing.T) {
	id := uuid.New()
	for _, tc := range []struct {
		name string
		job  *models.Job
		want Outcome
	}{
		{"nil job", nil, OutcomeError},
		{"wrong ID", testJob(uuid.New(), "https://db.test", models.StatusPending), OutcomeSkipped},
		{"URL mismatch", testJob(id, "https://other.test", models.StatusPending), OutcomeSkipped},
		{"unknown status", testJob(id, "https://db.test", models.JobStatus("future")), OutcomeSkipped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRepo{job: tc.job}
			a, s, out := &testAcker{}, &testScraper{}, &testReceiver{}
			p := makeProcessor(t, r, a, s, out)
			got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "19-1", payload(id, "https://db.test")))
			if got != tc.want || err == nil || r.claimCalls != 0 || s.calls != 0 || len(out.results) != 0 || len(a.ids) != 0 {
				t.Fatalf("result=%v err=%v claim=%d scrape=%d output=%d ack=%d", got, err, r.claimCalls, s.calls, len(out.results), len(a.ids))
			}
		})
	}
}

func TestProcessGetFailureHasNoSideEffects(t *testing.T) {
	id := uuid.New()
	r := &testRepo{getErr: errors.New("database password=secret")}
	a, s, out := &testAcker{}, &testScraper{}, &testReceiver{}
	p := makeProcessor(t, r, a, s, out)
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "20-1", payload(id, "https://db.test")))
	if got != OutcomeError || err == nil || strings.Contains(err.Error(), "secret") || r.claimCalls != 0 || s.calls != 0 || len(out.results) != 0 || len(a.ids) != 0 {
		t.Fatalf("result=%v err=%v claim=%d scrape=%d output=%d ack=%d", got, err, r.claimCalls, s.calls, len(out.results), len(a.ids))
	}
}

func TestProcessSuccessLogUsesScrapeBytesAndExcludesContent(t *testing.T) {
	id := uuid.New()
	var logOutput strings.Builder
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	r := &testRepo{job: testJob(id, "https://db.test", models.StatusPending), claim: true}
	a := &testAcker{}
	s := &testScraper{article: scraper.Article{Text: "article-text-secret", Title: "title-secret"}, stats: scraper.ScrapeStats{BytesRead: 37}}
	out := &testReceiver{}
	p, err := NewProcessor(r, a, s, out, Options{DBTimeout: time.Second, QueueOperationTimeout: time.Second, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	got, err := p.Process(context.Background(), context.Background(), testDelivery(id, "21-1", payload(id, "https://db.test")))
	if got != OutcomeSucceeded || err != nil {
		t.Fatalf("result=%v err=%v", got, err)
	}
	logs := logOutput.String()
	if !strings.Contains(logs, "bytes_read=37") || strings.Contains(logs, "article-text-secret") || strings.Contains(logs, "title-secret") || strings.Contains(logs, "https://db.test") {
		t.Fatalf("unsafe or inaccurate logs: %s", logs)
	}
}

func TestNewProcessorRejectsMissingDependenciesAndTimeouts(t *testing.T) {
	valid := Options{DBTimeout: time.Second, QueueOperationTimeout: time.Second}
	r, a, s, out := &testRepo{}, &testAcker{}, &testScraper{}, &testReceiver{}
	for _, tc := range []struct {
		name     string
		repo     Repo
		acker    Acker
		scraper  ArticleScraper
		receiver Receiver
		options  Options
	}{
		{"nil dependency", nil, a, s, out, valid},
		{"typed nil dependency", (*testRepo)(nil), a, s, out, valid},
		{"zero DB timeout", r, a, s, out, Options{QueueOperationTimeout: time.Second}},
		{"zero queue timeout", r, a, s, out, Options{DBTimeout: time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewProcessor(tc.repo, tc.acker, tc.scraper, tc.receiver, tc.options); err == nil {
				t.Fatal("expected constructor error")
			}
		})
	}
}
