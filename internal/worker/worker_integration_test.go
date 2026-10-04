//go:build integration

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"inteldigest/internal/db"
	"inteldigest/internal/models"
	"inteldigest/internal/queue"
	"inteldigest/internal/scraper"
)

const integrationDBName = "inteldigest_test"

var integrationDB *pgxpool.Pool

// TestMain owns only inteldigest_test. The configured URL must name the usual
// application database so the admin connection can be derived and guarded.
func TestMain(m *testing.M) {
	dsn := os.Getenv("DATABASE_URL")
	parsed, err := url.Parse(dsn)
	if err != nil || parsed.Path != "/inteldigest" {
		fmt.Fprintln(os.Stderr, "integration tests require DATABASE_URL naming inteldigest")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	adminURL := *parsed
	adminURL.Path = "/postgres"
	adminCfg, err := pgx.ParseConfig(adminURL.String())
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid admin database URL")
		os.Exit(2)
	}
	admin, err := pgx.ConnectConfig(ctx, adminCfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot connect to PostgreSQL admin database")
		os.Exit(2)
	}
	if err := resetIntegrationDB(ctx, admin); err != nil {
		_ = admin.Close(ctx)
		fmt.Fprintln(os.Stderr, "cannot prepare isolated integration database")
		os.Exit(2)
	}
	_ = admin.Close(ctx)
	cancel()
	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelSetup()
	testURL := *parsed
	testURL.Path = "/" + integrationDBName
	integrationDB, err = pgxpool.New(setupCtx, testURL.String())
	if err != nil || integrationDB.Config().ConnConfig.Database != integrationDBName {
		fmt.Fprintln(os.Stderr, "cannot connect to guarded integration database")
		os.Exit(2)
	}
	migration, err := os.ReadFile(filepath.Join("..", "..", "migrations", "001_create_jobs.up.sql"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot read jobs migration")
		os.Exit(2)
	}
	if _, err := integrationDB.Exec(setupCtx, string(migration)); err != nil {
		integrationDB.Close()
		fmt.Fprintln(os.Stderr, "cannot apply jobs migration to isolated integration database")
		os.Exit(2)
	}
	code := m.Run()
	integrationDB.Close()
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancelCleanup()
	admin, err = pgx.ConnectConfig(cleanupCtx, adminCfg)
	if err == nil {
		if dropErr := dropIntegrationDB(cleanupCtx, admin); dropErr != nil {
			fmt.Fprintln(os.Stderr, "cannot clean isolated integration database")
			code = 1
		}
		_ = admin.Close(cleanupCtx)
	}
	os.Exit(code)
}

func resetIntegrationDB(ctx context.Context, admin *pgx.Conn) error {
	if admin.Config().Database != "postgres" {
		return errors.New("admin database guard failed")
	}
	if err := dropIntegrationDB(ctx, admin); err != nil {
		return err
	}
	_, err := admin.Exec(ctx, "CREATE DATABASE "+integrationDBName)
	return err
}
func dropIntegrationDB(ctx context.Context, admin *pgx.Conn) error {
	if admin.Config().Database != "postgres" {
		return errors.New("admin database guard failed")
	}
	_, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+integrationDBName)
	return err
}
func guardedTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	if integrationDB == nil || integrationDB.Config().ConnConfig.Database != integrationDBName {
		t.Fatal("refusing database operation outside inteldigest_test")
	}
	return integrationDB
}

type integrationEnv struct {
	ctx    context.Context
	admin  *redis.Client
	stream string
	group  string
	url    string
}

func setupWorkerIntegration(t *testing.T) *integrationEnv {
	t.Helper()
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Fatal("REDIS_URL is required; integration checks must not be skipped")
	}
	parsed, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal("invalid REDIS_URL")
	}
	admin := redis.NewClient(parsed)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	stream := "inteldigest:worker-integration:" + uuid.NewString()
	group := "group:" + uuid.NewString()
	if err := admin.Ping(ctx).Err(); err != nil {
		cancel()
		_ = admin.Close()
		t.Fatal("Redis is unavailable")
	}
	if err := guardedTestPool(t).Ping(ctx); err != nil {
		cancel()
		_ = admin.Close()
		t.Fatal("PostgreSQL test database is unavailable")
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		if err := admin.Del(cleanupCtx, stream).Err(); err != nil {
			t.Errorf("delete owned Redis stream: %v", err)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("close Redis inspection client: %v", err)
		}
		cancel()
	})
	return &integrationEnv{ctx: ctx, admin: admin, stream: stream, group: group, url: redisURL}
}
func (e *integrationEnv) consumer(t *testing.T, name string) *queue.Consumer {
	t.Helper()
	c, err := queue.NewConsumer(e.ctx, queue.ConsumerOptions{RedisURL: e.url, Stream: e.stream, Group: e.group, Name: name, Block: 75 * time.Millisecond, OperationTimeout: time.Second, DialTimeout: time.Second, PoolSize: 4})
	if err != nil {
		t.Fatal("connect integration consumer")
	}
	if _, err := c.EnsureGroup(e.ctx); err != nil {
		_ = c.Close()
		t.Fatal("ensure integration consumer group")
	}
	return c
}
func (e *integrationEnv) publish(t *testing.T, id uuid.UUID, rawURL string) string {
	t.Helper()
	data, _ := json.Marshal(queue.Message{SchemaVersion: queue.SchemaVersion, JobID: id, URL: rawURL})
	ctx, cancel := context.WithTimeout(e.ctx, time.Second)
	defer cancel()
	entry, err := e.admin.XAdd(ctx, &redis.XAddArgs{Stream: e.stream, Values: map[string]interface{}{"data": string(data)}}).Result()
	if err != nil {
		t.Fatal("publish isolated worker test message")
	}
	return entry
}
func (e *integrationEnv) pending(t *testing.T) []redis.XPendingExt {
	t.Helper()
	ctx, cancel := context.WithTimeout(e.ctx, time.Second)
	defer cancel()
	got, err := e.admin.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: e.stream, Group: e.group, Start: "-", End: "+", Count: 100}).Result()
	if err != nil {
		t.Fatal("inspect owned consumer pending entries")
	}
	return got
}
func seedJob(t *testing.T, rawURL string, status models.JobStatus, diagnostic *string) uuid.UUID {
	t.Helper()
	pool := guardedTestPool(t)
	id := uuid.New()
	_, err := pool.Exec(context.Background(), `INSERT INTO jobs(id,url,status,error) VALUES($1,$2,$3,$4)`, id, rawURL, status, diagnostic)
	if err != nil {
		t.Fatal("seed guarded integration job")
	}
	return id
}

type scrapeStub struct {
	mu      sync.Mutex
	article scraper.Article
	stats   scraper.ScrapeStats
	err     error
	entered chan struct{}
	release <-chan struct{}
	calls   int
	urls    []string
}

func (s *scrapeStub) ScrapeWithStats(ctx context.Context, rawURL string) (scraper.Article, scraper.ScrapeStats, error) {
	s.mu.Lock()
	s.calls++
	s.urls = append(s.urls, rawURL)
	s.mu.Unlock()
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	if s.release != nil {
		select {
		case <-s.release:
		case <-ctx.Done():
			return scraper.Article{}, scraper.ScrapeStats{}, ctx.Err()
		}
	}
	return s.article, s.stats, s.err
}

type outcomeRecorder struct{ results chan Result }

func (r *outcomeRecorder) Receive(ctx context.Context, result Result) error {
	select {
	case r.results <- result:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type observedProcessor struct {
	inner *Processor
	done  chan Outcome
}

func (p *observedProcessor) Process(admission, job context.Context, d queue.Delivery) (Outcome, error) {
	outcome, err := p.inner.Process(admission, job, d)
	p.done <- outcome
	return outcome, err
}
func buildPool(t *testing.T, c *queue.Consumer, s *scrapeStub, results chan Result, size int, dbrepo Repo, observed chan Outcome) *Pool {
	t.Helper()
	inner, err := NewProcessor(dbrepo, c, s, &outcomeRecorder{results: results}, Options{DBTimeout: time.Second, QueueOperationTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewPool(c, &observedProcessor{inner: inner, done: observed}, PoolOptions{PoolSize: size, ShutdownTimeout: time.Second, BackoffDelay: 20 * time.Millisecond, Logger: slog.New(slog.NewTextHandler(&strings.Builder{}, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func runPool(ctx context.Context, p *Pool) <-chan error {
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	return done
}
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case v := <-ch:
		return v
	case <-timer.C:
		t.Fatal("timed out waiting for bounded integration event")
		var zero T
		return zero
	}
}
func readJob(t *testing.T, id uuid.UUID) *models.Job {
	t.Helper()
	job, err := db.NewRepository(guardedTestPool(t)).GetJobByID(context.Background(), id)
	if err != nil {
		t.Fatal("read guarded integration job")
	}
	return job
}

// barrierRepo ensures both processors see the committed pending snapshot before
// either atomic claim, while allowing the claim-loser requery through afterward.
type barrierRepo struct {
	Repo
	mu      sync.Mutex
	seen    int
	release chan struct{}
}

func (r *barrierRepo) GetJobByID(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	job, err := r.Repo.GetJobByID(ctx, id)
	r.mu.Lock()
	r.seen++
	n := r.seen
	if n == 2 {
		close(r.release)
	}
	r.mu.Unlock()
	if n <= 2 {
		select {
		case <-r.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return job, err
}
func TestWorkerIntegrationDuplicateClaimAndScrapeErrorRemainUnacked(t *testing.T) {
	e := setupWorkerIntegration(t)
	rawURL := "https://integration.invalid/article"
	id := seedJob(t, rawURL, models.StatusPending, nil)
	first := e.publish(t, id, rawURL)
	second := e.publish(t, id, rawURL)
	c := e.consumer(t, "duplicate-"+uuid.NewString())
	defer c.Close()
	stub := &scrapeStub{article: scraper.Article{Text: "deterministic fixture", RequestedURL: rawURL, FinalURL: rawURL, Title: "fixture"}, stats: scraper.ScrapeStats{BytesRead: 37}}
	results := make(chan Result, 2)
	outcomes := make(chan Outcome, 2)
	barrier := &barrierRepo{Repo: db.NewRepository(guardedTestPool(t)), release: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := runPool(ctx, buildPool(t, c, stub, results, 2, barrier, outcomes))
	a, b := await(t, outcomes), await(t, outcomes)
	if !((a == OutcomeSucceeded && b == OutcomeProcessingDuplicate) || (b == OutcomeSucceeded && a == OutcomeProcessingDuplicate)) {
		t.Fatalf("duplicate outcomes %q,%q", a, b)
	}
	result := await(t, results)
	if result.JobID != id || result.Article == nil || result.Article.Text != "deterministic fixture" {
		t.Fatalf("unexpected success result: %+v", result)
	}
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal("join duplicate pool")
	}
	job := readJob(t, id)
	if job.Status != models.StatusProcessing || job.Error != nil {
		t.Fatalf("duplicate final job=%+v", job)
	}
	pending := e.pending(t)
	if len(pending) != 2 || (pending[0].ID != first && pending[1].ID != first) || (pending[0].ID != second && pending[1].ID != second) {
		t.Fatalf("duplicate PEL=%+v", pending)
	}
	if stub.calls != 1 {
		t.Fatalf("scrape calls=%d, want one", stub.calls)
	}

	// Failure is a typed deterministic fixture; the raw cause and URL must not
	// enter persisted diagnosis or returned operational text.
	failureURL := "https://failure.invalid/private?token=never-store"
	secret := "cause-private-marker"
	failID := seedJob(t, failureURL, models.StatusPending, nil)
	failEntry := e.publish(t, failID, failureURL)
	c2 := e.consumer(t, "failure-"+uuid.NewString())
	defer c2.Close()
	failure := &scrapeStub{err: &scraper.ScrapeError{Kind: scraper.ScrapeErrorHTTPStatus, Stage: scraper.ScrapeStageFetch, HTTPStatus: 500, Cause: errors.New(secret)}}
	fr := make(chan Result, 1)
	fo := make(chan Outcome, 1)
	fc, stop := context.WithCancel(context.Background())
	fdone := runPool(fc, buildPool(t, c2, failure, fr, 1, db.NewRepository(guardedTestPool(t)), fo))
	if got := await(t, fo); got != OutcomeScrapeFailed {
		t.Fatalf("failure outcome=%q", got)
	}
	delivered := await(t, fr)
	if delivered.Error == nil || delivered.Article != nil {
		t.Fatalf("failure result=%+v", delivered)
	}
	stop()
	if err := await(t, fdone); err != nil {
		t.Fatal("join failure pool")
	}
	persisted := readJob(t, failID)
	if persisted.Status != models.StatusProcessing || persisted.Error == nil || len([]rune(*persisted.Error)) > 1024 || strings.Contains(*persisted.Error, secret) || strings.Contains(*persisted.Error, "failure.invalid") {
		t.Fatalf("unsafe scrape diagnosis: %+v", persisted)
	}
	fp := e.pending(t)
	found := false
	for _, p := range fp {
		if p.ID == failEntry {
			found = true
		}
	}
	if !found {
		t.Fatalf("failed scrape entry was acknowledged: %+v", fp)
	}
}

func TestWorkerIntegrationRestartDoesNotRecoverPendingAndTerminalDuplicatesAck(t *testing.T) {
	e := setupWorkerIntegration(t)
	stuckURL := "https://restart.invalid/a"
	stuck := seedJob(t, stuckURL, models.StatusPending, nil)
	stuckEntry := e.publish(t, stuck, stuckURL)
	oldConsumer := "old-" + uuid.NewString()
	c := e.consumer(t, oldConsumer)
	blocked := make(chan struct{}, 1)
	oldScraper := &scrapeStub{entered: blocked, release: make(chan struct{})}
	oldResults := make(chan Result, 2)
	oldOutcomes := make(chan Outcome, 2)
	root, cancel := context.WithCancel(context.Background())
	oldDone := runPool(root, buildPool(t, c, oldScraper, oldResults, 1, db.NewRepository(guardedTestPool(t)), oldOutcomes))
	await(t, blocked)
	cancel()
	if err := await(t, oldDone); err != nil {
		t.Fatal("join old pool")
	}
	if err := c.Close(); err != nil {
		t.Fatal("close old consumer")
	}
	oldPending := e.pending(t)
	if len(oldPending) != 1 || oldPending[0].ID != stuckEntry || oldPending[0].Consumer != oldConsumer {
		t.Fatalf("old pending entry/owner not retained: %+v", oldPending)
	}
	newURL := "https://restart.invalid/b"
	fresh := seedJob(t, newURL, models.StatusPending, nil)
	freshEntry := e.publish(t, fresh, newURL)
	newConsumer := "new-" + uuid.NewString()
	c2 := e.consumer(t, newConsumer)
	defer c2.Close()
	newStub := &scrapeStub{article: scraper.Article{Text: "new only", RequestedURL: newURL, FinalURL: newURL}, stats: scraper.ScrapeStats{BytesRead: 11}}
	newResults := make(chan Result, 2)
	newOutcomes := make(chan Outcome, 2)
	ctx, stop := context.WithCancel(context.Background())
	done := runPool(ctx, buildPool(t, c2, newStub, newResults, 1, db.NewRepository(guardedTestPool(t)), newOutcomes))
	if got := await(t, newOutcomes); got != OutcomeSucceeded {
		t.Fatalf("new job outcome=%q", got)
	}
	if got := await(t, newResults); got.JobID != fresh || got.Article == nil {
		t.Fatalf("new job result=%+v", got)
	}
	stop()
	if err := await(t, done); err != nil {
		t.Fatal("join restarted pool")
	}
	if readJob(t, stuck).Status != models.StatusProcessing || readJob(t, fresh).Status != models.StatusProcessing || newStub.calls != 1 || oldScraper.calls != 1 {
		t.Fatalf("restart states stuck=%+v fresh=%+v oldScrapes=%d newScrapes=%d", readJob(t, stuck), readJob(t, fresh), oldScraper.calls, newStub.calls)
	}
	remaining := e.pending(t)
	ids := map[string]redis.XPendingExt{}
	for _, p := range remaining {
		ids[p.ID] = p
	}
	if len(remaining) != 2 || ids[stuckEntry].ID != stuckEntry || ids[stuckEntry].Consumer != oldConsumer || ids[freshEntry].ID != freshEntry || ids[freshEntry].Consumer != newConsumer {
		t.Fatalf("restart PEL IDs/owners=%+v", remaining)
	}

	for _, status := range []models.JobStatus{models.StatusCompleted, models.StatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			terminalEnv := setupWorkerIntegration(t)
			u := "https://terminal.invalid/" + string(status)
			terminal := seedJob(t, u, status, nil)
			entry := terminalEnv.publish(t, terminal, u)
			tc := terminalEnv.consumer(t, "terminal-"+uuid.NewString())
			defer tc.Close()
			stub := &scrapeStub{}
			results := make(chan Result, 1)
			outs := make(chan Outcome, 1)
			ctx, cancel := context.WithCancel(context.Background())
			poolDone := runPool(ctx, buildPool(t, tc, stub, results, 1, db.NewRepository(guardedTestPool(t)), outs))
			if got := await(t, outs); got != OutcomeTerminalDuplicate {
				t.Fatalf("terminal outcome=%q", got)
			}
			cancel()
			if err := await(t, poolDone); err != nil {
				t.Fatal("join terminal pool")
			}
			job := readJob(t, terminal)
			if job.Status != status || job.Result != nil || job.Error != nil || stub.calls != 0 || len(results) != 0 {
				t.Fatalf("terminal duplicate changed job or scraped: job=%+v calls=%d outputs=%d", job, stub.calls, len(results))
			}
			if pending := terminalEnv.pending(t); len(pending) != 0 {
				t.Fatalf("terminal duplicate PEL after ACK: %+v (entry %s)", pending, entry)
			}
		})
	}
}
