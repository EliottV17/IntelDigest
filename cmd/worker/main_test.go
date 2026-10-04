package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"inteldigest/internal/config"
	"inteldigest/internal/db"
	"inteldigest/internal/queue"
	"inteldigest/internal/scraper"
	"inteldigest/internal/worker"
)

type fakeDB struct {
	mu      *sync.Mutex
	events  *[]string
	pingErr error
	pingCtx context.Context
	onPing  func(context.Context)
}

func (*fakeDB) QueryRow(context.Context, string, ...any) pgx.Row { return nil }
func (*fakeDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (f *fakeDB) Ping(ctx context.Context) error {
	if f.mu != nil {
		f.mu.Lock()
		*f.events = append(*f.events, "db-ping")
		f.mu.Unlock()
	}
	f.pingCtx = ctx
	if f.onPing != nil {
		f.onPing(ctx)
	}
	return f.pingErr
}
func (f *fakeDB) Close() {
	if f.mu != nil {
		f.mu.Lock()
		*f.events = append(*f.events, "db-close")
		f.mu.Unlock()
	}
}

type fakeConsumer struct {
	mu                   *sync.Mutex
	events               *[]string
	groupErr, errorClose error
	ensureCtx            context.Context
}

func (*fakeConsumer) Read(context.Context) (queue.Delivery, bool, error) {
	return queue.Delivery{}, false, nil
}
func (*fakeConsumer) Ack(context.Context, queue.Delivery) error { return nil }
func (f *fakeConsumer) EnsureGroup(ctx context.Context) (bool, error) {
	f.ensureCtx = ctx
	if f.mu != nil {
		f.mu.Lock()
		*f.events = append(*f.events, "group")
		f.mu.Unlock()
	}
	return true, f.groupErr
}
func (f *fakeConsumer) Close() error {
	if f.mu != nil {
		f.mu.Lock()
		*f.events = append(*f.events, "consumer-close")
		f.mu.Unlock()
	}
	return f.errorClose
}

type fakeScraper struct{}

func (fakeScraper) ScrapeWithStats(context.Context, string) (scraper.Article, scraper.ScrapeStats, error) {
	return scraper.Article{}, scraper.ScrapeStats{}, nil
}

type fakeProcessor struct{}

func (fakeProcessor) Process(context.Context, context.Context, queue.Delivery) (worker.Outcome, error) {
	return "", nil
}

type fakePool struct {
	run    func(context.Context) error
	mu     *sync.Mutex
	events *[]string
}

func (f *fakePool) Run(ctx context.Context) error {
	if f.mu != nil {
		f.mu.Lock()
		*f.events = append(*f.events, "run-start")
		f.mu.Unlock()
	}
	if f.run != nil {
		return f.run(ctx)
	}
	return nil
}

func TestRunMapsValidatedWorkerSettingsAndSignalLifecycle(t *testing.T) {
	cfg := testConfig()
	var events []string
	var mu sync.Mutex
	appendEvent := func(s string) { mu.Lock(); events = append(events, s); mu.Unlock() }
	d := &fakeDB{mu: &mu, events: &events}
	c := &fakeConsumer{mu: &mu, events: &events}
	p := &fakePool{mu: &mu, events: &events}
	var gotConsumer queue.ConsumerOptions
	var gotScraper scraper.ScraperOptions
	var gotProcessor worker.Options
	var gotPool worker.PoolOptions
	notifyCount, stopCount := 0, 0
	var logOutput strings.Builder
	logger := slog.New(slog.NewJSONHandler(&logOutput, nil))
	deps := dependencies{
		loadConfig: func() (*config.WorkerConfig, error) { appendEvent("config"); return cfg, nil },
		notifyContext: func(parent context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
			notifyCount++
			if !reflect.DeepEqual(signals, []os.Signal{os.Interrupt, syscall.SIGTERM}) {
				t.Fatalf("signals=%v", signals)
			}
			appendEvent("notify")
			ctx, cancel := context.WithCancel(parent)
			return ctx, func() { stopCount++; cancel() }
		},
		newDB: func(context.Context, string) (dbHandle, error) { appendEvent("db-new"); return d, nil },
		newConsumer: func(_ context.Context, o queue.ConsumerOptions) (consumerHandle, error) {
			appendEvent("consumer-new")
			gotConsumer = o
			return c, nil
		},
		newScraper: func(o scraper.ScraperOptions) (scraperHandle, error) {
			appendEvent("scraper-new")
			gotScraper = o
			return fakeScraper{}, nil
		},
		newProcessor: func(_ worker.Repo, _ worker.Acker, _ worker.ArticleScraper, r worker.Receiver, o worker.Options) (processorHandle, error) {
			if invalid(r) {
				t.Fatal("nil receiver")
			}
			gotProcessor = o
			return fakeProcessor{}, nil
		},
		newPool: func(_ worker.QueueReader, _ worker.JobProcessor, o worker.PoolOptions) (poolHandle, error) {
			gotPool = o
			return p, nil
		}, logger: logger,
	}
	if err := runWithDependencies(context.Background(), deps); err != nil {
		t.Fatalf("run: %v", err)
	}
	if notifyCount != 1 || stopCount != 1 {
		t.Fatalf("notify/stop=%d/%d", notifyCount, stopCount)
	}
	if len(events) < 2 || events[0] != "notify" || events[1] != "config" {
		t.Fatalf("registration order: %v", events)
	}
	wantConsumer := queue.ConsumerOptions{RedisURL: cfg.RedisURL, Stream: cfg.RedisStream, Group: cfg.ConsumerGroup, Name: cfg.ConsumerName, Block: cfg.StreamBlock, OperationTimeout: cfg.QueueOperationTimeout, DialTimeout: cfg.RedisDialTimeout, PoolSize: cfg.PoolSize}
	if !reflect.DeepEqual(gotConsumer, wantConsumer) {
		t.Errorf("consumer=%+v want=%+v", gotConsumer, wantConsumer)
	}
	wantScraper := scraper.ScraperOptions{ConnectTimeout: cfg.ScraperConnectTimeout, TLSTimeout: cfg.ScraperTLSTimeout, HeaderTimeout: cfg.ScraperHeaderTimeout, ReadTimeout: cfg.ScraperReadTimeout, RequestTimeout: cfg.ScraperRequestTimeout, MaxBodyBytes: cfg.ScraperMaxBodyBytes, MaxRedirects: cfg.ScraperMaxRedirects}
	if !reflect.DeepEqual(gotScraper, wantScraper) {
		t.Errorf("scraper=%+v want=%+v", gotScraper, wantScraper)
	}
	if gotProcessor.DBTimeout != cfg.DBTimeout || gotProcessor.QueueOperationTimeout != cfg.QueueOperationTimeout || gotProcessor.Logger == nil {
		t.Errorf("processor options=%+v", gotProcessor)
	}
	gotProcessor.Logger.Info("logger_mapping_probe")
	if !strings.Contains(logOutput.String(), `"consumer_group":"group-secret"`) || !strings.Contains(logOutput.String(), `"consumer_name":"name-secret"`) {
		t.Errorf("processor logger context missing: %s", logOutput.String())
	}
	if _, ok := d.pingCtx.Deadline(); !ok {
		t.Error("DB ping context was not bounded")
	}
	if _, ok := c.ensureCtx.Deadline(); !ok {
		t.Error("group ensure context was not bounded")
	}
	if gotPool.PoolSize != cfg.PoolSize || gotPool.ShutdownTimeout != cfg.ShutdownTimeout || gotPool.BackoffDelay != time.Second || gotPool.Logger == nil {
		t.Errorf("pool options=%+v", gotPool)
	}
	wantEvents := []string{"notify", "config", "db-new", "db-ping", "consumer-new", "group", "scraper-new", "run-start", "consumer-close", "db-close"}
	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("lifecycle=%v want=%v", events, wantEvents)
	}
	for _, secret := range []string{cfg.DatabaseURL, cfg.RedisURL, cfg.RedisStream} {
		if strings.Contains(logOutput.String(), secret) {
			t.Errorf("startup logs leaked credential: %s", logOutput.String())
		}
	}
}

func TestDatabaseOpenGetsBoundedValueContextCanceledBeforeIndependentPing(t *testing.T) {
	cfg := testConfig()
	root := context.WithValue(context.Background(), struct{}{}, "root-value")
	dbFake := &fakeDB{}
	var openCtx context.Context
	pingChecked := false
	dbFake.onPing = func(ctx context.Context) {
		if ctx.Err() != nil {
			t.Errorf("Ping context canceled on entry: %v", ctx.Err())
		}
		if _, ok := ctx.Deadline(); !ok {
			t.Error("Ping has no independent timeout")
		}
		pingChecked = true
	}
	dep := baseDependencies(func(string) {}, cfg, dbFake, &fakeConsumer{})
	dep.notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return root, func() {}
	}
	dep.newDB = func(ctx context.Context, _ string) (dbHandle, error) {
		openCtx = ctx
		if ctx.Value(struct{}{}) != "root-value" {
			t.Error("DB open context did not preserve root values")
		}
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("DB open context has no finite deadline")
		} else if time.Until(deadline) > cfg.DBTimeout || time.Until(deadline) <= 0 {
			t.Errorf("DB open deadline exceeds budget or is already expired: %v", deadline)
		}
		return dbFake, nil
	}
	if err := runWithDependencies(context.Background(), dep); err != nil {
		t.Fatalf("run: %v", err)
	}
	if openCtx == nil || !errors.Is(openCtx.Err(), context.Canceled) {
		t.Fatalf("open context was not canceled after factory return: %v", openCtx)
	}
	if !pingChecked {
		t.Fatal("Ping was not called")
	}
	if !errors.Is(dbFake.pingCtx.Err(), context.Canceled) {
		t.Fatalf("Ping context was not independently canceled after Ping: %v", dbFake.pingCtx.Err())
	}
}

func TestDatabaseOpenHonorsEarlierRootDeadline(t *testing.T) {
	cfg := testConfig()
	cfg.DBTimeout = time.Minute
	root, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var openDeadline time.Time
	dep := baseDependencies(func(string) {}, cfg, &fakeDB{}, &fakeConsumer{})
	dep.notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) { return root, func() {} }
	dep.newDB = func(ctx context.Context, _ string) (dbHandle, error) {
		openDeadline, _ = ctx.Deadline()
		return &fakeDB{}, nil
	}
	if err := runWithDependencies(context.Background(), dep); err != nil {
		t.Fatalf("run: %v", err)
	}
	rootDeadline, ok := root.Deadline()
	if !ok || !openDeadline.Equal(rootDeadline) {
		t.Fatalf("open deadline=%v root deadline=%v", openDeadline, rootDeadline)
	}
}

func TestRootCanceledDuringDatabaseOpenSkipsPingAndLaterFactories(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	var events []string
	d := &fakeDB{mu: &mu, events: &events}
	dep := baseDependencies(func(string) {}, testConfig(), d, &fakeConsumer{})
	dep.notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) { return root, func() {} }
	dep.newDB = func(context.Context, string) (dbHandle, error) { cancel(); return d, nil }
	err := runWithDependencies(context.Background(), dep)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(events, []string{"db-close"}) {
		t.Fatalf("canceled startup continued or cleanup changed: %v", events)
	}
}

func TestDatabaseOpenDeadlinePropagatesAndClosesHandleReturnedWithError(t *testing.T) {
	cfg := testConfig()
	cfg.DBTimeout = 25 * time.Millisecond
	var mu sync.Mutex
	var events []string
	d := &fakeDB{mu: &mu, events: &events}
	dep := baseDependencies(func(string) {}, cfg, d, &fakeConsumer{})
	dep.newDB = func(ctx context.Context, _ string) (dbHandle, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("database open has no deadline")
		}
		<-ctx.Done()
		return d, ctx.Err()
	}
	err := runWithDependencies(context.Background(), dep)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("database timeout cause lost: %v", err)
	}
	if !reflect.DeepEqual(events, []string{"db-close"}) {
		t.Fatalf("handle not closed exactly once or startup continued: %v", events)
	}
}

func TestStartupFailureCleanupAndNoLaterFactories(t *testing.T) {
	sentinel := errors.New("postgres://user:password@host secret")
	cases := []struct {
		name    string
		fail    string
		want    []string
		isCause bool
	}{
		{"config", "config", []string{"notify", "config"}, true},
		{"db factory", "db", []string{"notify", "config", "db-new"}, true},
		{"db ping", "ping", []string{"notify", "config", "db-new", "db-ping", "db-close"}, true},
		{"consumer factory", "consumer", []string{"notify", "config", "db-new", "db-ping", "consumer-new", "db-close"}, true},
		{"group ensure", "group", []string{"notify", "config", "db-new", "db-ping", "consumer-new", "group", "consumer-close", "db-close"}, true},
		{"scraper", "scraper", []string{"notify", "config", "db-new", "db-ping", "consumer-new", "group", "scraper-new", "consumer-close", "db-close"}, true},
		{"processor", "processor", []string{"notify", "config", "db-new", "db-ping", "consumer-new", "group", "scraper-new", "consumer-close", "db-close"}, true},
		{"pool", "pool", []string{"notify", "config", "db-new", "db-ping", "consumer-new", "group", "scraper-new", "consumer-close", "db-close"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var events []string
			event := func(s string) { mu.Lock(); events = append(events, s); mu.Unlock() }
			d := &fakeDB{mu: &mu, events: &events}
			c := &fakeConsumer{mu: &mu, events: &events}
			cfg := testConfig()
			dep := baseDependencies(event, cfg, d, c)
			dep.loadConfig = func() (*config.WorkerConfig, error) {
				event("config")
				if tc.fail == "config" {
					return nil, sentinel
				}
				return cfg, nil
			}
			dep.newDB = func(context.Context, string) (dbHandle, error) {
				event("db-new")
				if tc.fail == "db" {
					return nil, sentinel
				}
				return d, nil
			}
			if tc.fail == "ping" {
				d.pingErr = sentinel
			}
			dep.newConsumer = func(context.Context, queue.ConsumerOptions) (consumerHandle, error) {
				event("consumer-new")
				if tc.fail == "consumer" {
					return nil, sentinel
				}
				return c, nil
			}
			if tc.fail == "group" {
				c.groupErr = sentinel
			}
			dep.newScraper = func(scraper.ScraperOptions) (scraperHandle, error) {
				event("scraper-new")
				if tc.fail == "scraper" {
					return nil, sentinel
				}
				return fakeScraper{}, nil
			}
			dep.newProcessor = func(worker.Repo, worker.Acker, worker.ArticleScraper, worker.Receiver, worker.Options) (processorHandle, error) {
				if tc.fail == "processor" {
					return nil, sentinel
				}
				return fakeProcessor{}, nil
			}
			dep.newPool = func(worker.QueueReader, worker.JobProcessor, worker.PoolOptions) (poolHandle, error) {
				if tc.fail == "pool" {
					return nil, sentinel
				}
				return &fakePool{}, nil
			}
			err := runWithDependencies(context.Background(), dep)
			if err == nil {
				t.Fatal("expected startup error")
			}
			if tc.isCause && !errors.Is(err, sentinel) {
				t.Errorf("error lost cause: %v", err)
			}
			if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "secret") {
				t.Errorf("returned error leaked cause: %v", err)
			}
			if !reflect.DeepEqual(events, tc.want) {
				t.Errorf("events=%v want=%v", events, tc.want)
			}
		})
	}
}

func TestFactoryTypedNilPlusErrorDoesNotPanicOrLeak(t *testing.T) {
	sentinel := errors.New("secret factory error")
	t.Run("database", func(t *testing.T) {
		dep := baseDependencies(func(string) {}, testConfig(), &fakeDB{}, &fakeConsumer{})
		var typedNil *fakeDB
		dep.newDB = func(context.Context, string) (dbHandle, error) { return typedNil, sentinel }
		err := runWithDependencies(context.Background(), dep)
		if !errors.Is(err, sentinel) {
			t.Fatalf("lost cause: %v", err)
		}
	})
	t.Run("consumer", func(t *testing.T) {
		var mu sync.Mutex
		var events []string
		d := &fakeDB{mu: &mu, events: &events}
		var typedNil *fakeConsumer
		dep := baseDependencies(func(string) {}, testConfig(), d, &fakeConsumer{})
		dep.newConsumer = func(context.Context, queue.ConsumerOptions) (consumerHandle, error) { return typedNil, sentinel }
		err := runWithDependencies(context.Background(), dep)
		if !errors.Is(err, sentinel) {
			t.Fatalf("lost cause: %v", err)
		}
		if !reflect.DeepEqual(events, []string{"db-ping", "db-close"}) {
			t.Fatalf("events=%v", events)
		}
	})
}

func TestConsumerCloseErrorStillClosesDatabase(t *testing.T) {
	cause := errors.New("close secret redis url")
	var mu sync.Mutex
	var events []string
	d := &fakeDB{mu: &mu, events: &events}
	c := &fakeConsumer{mu: &mu, events: &events, errorClose: cause}
	dep := baseDependencies(func(string) {}, testConfig(), d, c)
	err := runWithDependencies(context.Background(), dep)
	if !errors.Is(err, cause) {
		t.Fatalf("close cause lost: %v", err)
	}
	if strings.Contains(err.Error(), "secret") {
		t.Fatalf("unsafe close error: %v", err)
	}
	if !reflect.DeepEqual(events, []string{"db-ping", "group", "consumer-close", "db-close"}) {
		t.Fatalf("events=%v", events)
	}
}

func TestAcquiredHandleReturnedWithFactoryErrorIsStillClosed(t *testing.T) {
	sentinel := errors.New("factory failed after acquire")
	t.Run("database", func(t *testing.T) {
		var mu sync.Mutex
		var events []string
		d := &fakeDB{mu: &mu, events: &events}
		dep := baseDependencies(func(string) {}, testConfig(), d, &fakeConsumer{})
		dep.newDB = func(context.Context, string) (dbHandle, error) { return d, sentinel }
		err := runWithDependencies(context.Background(), dep)
		if !errors.Is(err, sentinel) {
			t.Fatalf("cause=%v", err)
		}
		if !reflect.DeepEqual(events, []string{"db-close"}) {
			t.Fatalf("events=%v", events)
		}
	})
	t.Run("consumer", func(t *testing.T) {
		var mu sync.Mutex
		var events []string
		d := &fakeDB{mu: &mu, events: &events}
		c := &fakeConsumer{mu: &mu, events: &events}
		dep := baseDependencies(func(string) {}, testConfig(), d, c)
		dep.newConsumer = func(context.Context, queue.ConsumerOptions) (consumerHandle, error) { return c, sentinel }
		err := runWithDependencies(context.Background(), dep)
		if !errors.Is(err, sentinel) {
			t.Fatalf("cause=%v", err)
		}
		if !reflect.DeepEqual(events, []string{"db-ping", "consumer-close", "db-close"}) {
			t.Fatalf("events=%v", events)
		}
	})
}

func TestCanceledRootStopsBeforeOpeningNextServiceAndUnregisters(t *testing.T) {
	root, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testConfig()
	called := false
	stopped := 0
	dep := baseDependencies(func(string) {}, cfg, &fakeDB{}, &fakeConsumer{})
	dep.notifyContext = func(context.Context, ...os.Signal) (context.Context, context.CancelFunc) {
		return root, func() { stopped++ }
	}
	dep.loadConfig = func() (*config.WorkerConfig, error) { cancel(); return cfg, nil }
	dep.newDB = func(context.Context, string) (dbHandle, error) { called = true; return nil, nil }
	err := runWithDependencies(context.Background(), dep)
	if !errors.Is(err, context.Canceled) || called || stopped != 1 {
		t.Fatalf("err=%v dbCalled=%v unregister=%d", err, called, stopped)
	}
}

func TestRunWaitsForPoolAndSafeErrorsPreserveCause(t *testing.T) {
	var mu sync.Mutex
	var events []string
	d := &fakeDB{mu: &mu, events: &events}
	c := &fakeConsumer{mu: &mu, events: &events}
	cause := errors.New("redis://user:password@host article=private")
	p := &fakePool{mu: &mu, events: &events, run: func(context.Context) error {
		mu.Lock()
		events = append(events, "run-finish")
		mu.Unlock()
		return cause
	}}
	dep := baseDependencies(func(string) {}, testConfig(), d, c)
	dep.newPool = func(worker.QueueReader, worker.JobProcessor, worker.PoolOptions) (poolHandle, error) { return p, nil }
	err := runWithDependencies(context.Background(), dep)
	if !errors.Is(err, cause) {
		t.Fatalf("cause not preserved: %v", err)
	}
	if strings.Contains(err.Error(), "password") || strings.Contains(err.Error(), "private") {
		t.Fatalf("unsafe error: %v", err)
	}
	if !reflect.DeepEqual(events, []string{"db-ping", "group", "run-start", "run-finish", "consumer-close", "db-close"}) {
		t.Fatalf("run/close order=%v", events)
	}
}

func TestRunPreservesFatalMissingGroupAndExitClassification(t *testing.T) {
	var mu sync.Mutex
	var events []string
	d := &fakeDB{mu: &mu, events: &events}
	c := &fakeConsumer{mu: &mu, events: &events}
	missing := &queue.GroupMissingError{}
	dep := baseDependencies(func(string) {}, testConfig(), d, c)
	dep.newPool = func(worker.QueueReader, worker.JobProcessor, worker.PoolOptions) (poolHandle, error) {
		return &fakePool{run: func(context.Context) error { return missing }}, nil
	}
	err := runWithDependencies(context.Background(), dep)
	if err == nil || !queue.IsGroupMissing(err) || exitCode(err) != 1 {
		t.Fatalf("fatal group result=%v exit=%d", err, exitCode(err))
	}
	if strings.Contains(err.Error(), "NOGROUP") { // The safe classifier text does not expose raw Redis errors.
		t.Fatalf("unsafe fatal error text: %v", err)
	}
	if exitCode(nil) != 0 {
		t.Fatal("clean signal shutdown must exit successfully")
	}
}

func TestRunDoesNotCloseResourcesUntilPoolReturns(t *testing.T) {
	var mu sync.Mutex
	var events []string
	d := &fakeDB{mu: &mu, events: &events}
	c := &fakeConsumer{mu: &mu, events: &events}
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan error, 1)
	p := &fakePool{run: func(context.Context) error { close(started); <-release; return nil }}
	dep := baseDependencies(func(string) {}, testConfig(), d, c)
	dep.newPool = func(worker.QueueReader, worker.JobProcessor, worker.PoolOptions) (poolHandle, error) { return p, nil }
	go func() { finished <- runWithDependencies(context.Background(), dep) }()
	<-started
	mu.Lock()
	before := append([]string(nil), events...)
	mu.Unlock()
	if containsEvent(before, "consumer-close") || containsEvent(before, "db-close") {
		t.Fatalf("resource closed while Run active: %v", before)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatalf("run=%v", err)
	}
	mu.Lock()
	after := append([]string(nil), events...)
	mu.Unlock()
	if !containsEvent(after, "consumer-close") || !containsEvent(after, "db-close") {
		t.Fatalf("resources not closed after Run: %v", after)
	}
}
func containsEvent(events []string, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}

func TestMetadataReceiverIsSynchronousNonPersistentAndSafe(t *testing.T) {
	var output strings.Builder
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	receiver := metadataReceiver{logger: logger}
	article := scraper.Article{Text: "secret article body", Title: "secret title", Language: "private language", RequestedURL: "https://example.test/?token=secret", FinalURL: "https://example.test/final?secret=1"}
	if err := receiver.Receive(context.Background(), worker.Result{JobID: uuid.New(), Article: &article}); err != nil {
		t.Fatal(err)
	}
	scrapeErr := &scraper.ScrapeError{Kind: scraper.ScrapeErrorNetwork, Stage: scraper.ScrapeStageFetch, Message: "https://secret.test/?token=secret", Cause: errors.New("raw article cause")}
	if err := receiver.Receive(context.Background(), worker.Result{JobID: uuid.New(), Error: scrapeErr}); err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret article body", "secret title", "private language", "token=secret", "final?secret", "raw article cause", "secret.test"} {
		if strings.Contains(output.String(), secret) {
			t.Errorf("metadata log leaked %q: %s", secret, output.String())
		}
	}
	if err := receiver.Receive(context.Background(), worker.Result{}); err == nil {
		t.Fatal("accepted invalid result")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := receiver.Receive(ctx, worker.Result{JobID: uuid.New(), Article: &article}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error=%v", err)
	}
	unsafe := &scraper.ScrapeError{Kind: scraper.ScrapeErrorKind("secret-kind"), Stage: scraper.ScrapeStage("secret-stage"), HTTPStatus: 999, Message: "private"}
	if err := receiver.Receive(context.Background(), worker.Result{JobID: uuid.New(), Error: unsafe}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "secret-kind") || strings.Contains(output.String(), "secret-stage") || strings.Contains(output.String(), "private") {
		t.Fatalf("unknown metadata was not allowlisted: %s", output.String())
	}
}

func TestNilDependenciesAndNilConfigReturnErrorsWithoutPanic(t *testing.T) {
	if err := runWithDependencies(context.Background(), dependencies{}); err == nil {
		t.Fatal("nil dependencies accepted")
	}
	dep := baseDependencies(func(string) {}, nil, &fakeDB{}, &fakeConsumer{})
	if err := runWithDependencies(context.Background(), dep); err == nil {
		t.Fatal("nil config accepted")
	}
}

func baseDependencies(event func(string), cfg *config.WorkerConfig, d *fakeDB, c *fakeConsumer) dependencies {
	return dependencies{
		loadConfig: func() (*config.WorkerConfig, error) { event("config"); return cfg, nil },
		notifyContext: func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
			event("notify")
			return context.WithCancel(parent)
		},
		newDB: func(context.Context, string) (dbHandle, error) { event("db-new"); return d, nil },
		newConsumer: func(context.Context, queue.ConsumerOptions) (consumerHandle, error) {
			event("consumer-new")
			return c, nil
		},
		newScraper: func(scraper.ScraperOptions) (scraperHandle, error) { event("scraper-new"); return fakeScraper{}, nil },
		newProcessor: func(worker.Repo, worker.Acker, worker.ArticleScraper, worker.Receiver, worker.Options) (processorHandle, error) {
			return fakeProcessor{}, nil
		},
		newPool: func(worker.QueueReader, worker.JobProcessor, worker.PoolOptions) (poolHandle, error) {
			return &fakePool{}, nil
		},
	}
}
func testConfig() *config.WorkerConfig {
	return &config.WorkerConfig{Config: config.Config{DatabaseURL: "postgres://user:secret@db/inteldigest", RedisURL: "redis://user:secret@redis/0", RedisStream: "stream-secret", RedisDialTimeout: 3 * time.Second}, PoolSize: 7, ConsumerGroup: "group-secret", ConsumerName: "name-secret", StreamBlock: 1500 * time.Millisecond, QueueOperationTimeout: 6 * time.Second, QueueReadTimeout: 7500 * time.Millisecond, DBTimeout: 4 * time.Second, ShutdownTimeout: 11 * time.Second, ScraperConnectTimeout: 2 * time.Second, ScraperTLSTimeout: 3 * time.Second, ScraperHeaderTimeout: 4 * time.Second, ScraperReadTimeout: 5 * time.Second, ScraperRequestTimeout: 6 * time.Second, ScraperMaxBodyBytes: 123456, ScraperMaxBodyReadBytes: 123457, ScraperMaxRedirects: 2}
}

var _ db.DBTX = (*fakeDB)(nil)
var _ dbHandle = (*pgxpool.Pool)(nil)
