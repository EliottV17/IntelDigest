package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"syscall"
	"time"

	"inteldigest/internal/config"
	"inteldigest/internal/db"
	"inteldigest/internal/queue"
	"inteldigest/internal/scraper"
	"inteldigest/internal/worker"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dbHandle interface {
	db.DBTX
	Ping(context.Context) error
	Close()
}
type consumerHandle interface {
	worker.QueueReader
	worker.Acker
	EnsureGroup(context.Context) (bool, error)
	Close() error
}
type scraperHandle interface{ worker.ArticleScraper }
type processorHandle interface{ worker.JobProcessor }
type poolHandle interface{ Run(context.Context) error }

type dependencies struct {
	loadConfig    func() (*config.WorkerConfig, error)
	notifyContext func(context.Context, ...os.Signal) (context.Context, context.CancelFunc)
	newDB         func(context.Context, string) (dbHandle, error)
	newConsumer   func(context.Context, queue.ConsumerOptions) (consumerHandle, error)
	newScraper    func(scraper.ScraperOptions) (scraperHandle, error)
	newProcessor  func(worker.Repo, worker.Acker, worker.ArticleScraper, worker.Receiver, worker.Options) (processorHandle, error)
	newPool       func(worker.QueueReader, worker.JobProcessor, worker.PoolOptions) (poolHandle, error)
	logger        *slog.Logger
}

// main exits only after runWithDependencies has returned and all acquired
// resources and signal registrations have been released.
func main() { os.Exit(run()) }

func run() int {
	err := runWithDependencies(context.Background(), productionDependencies())
	if err != nil {
		slog.Error("worker_exit", "error_code", exitCodeLabel(err))
	}
	return exitCode(err)
}
func exitCode(err error) int {
	if err == nil {
		return 0
	}
	return 1
}
func exitCodeLabel(err error) string {
	var codedErr *codedError
	if errors.As(err, &codedErr) {
		return codedErr.code
	}
	return "worker_failed"
}

func productionDependencies() dependencies {
	return dependencies{
		loadConfig:    config.LoadWorker,
		notifyContext: signal.NotifyContext,
		newDB:         func(ctx context.Context, dsn string) (dbHandle, error) { return pgxpool.New(ctx, dsn) },
		newConsumer: func(ctx context.Context, options queue.ConsumerOptions) (consumerHandle, error) {
			return queue.NewConsumer(ctx, options)
		},
		newScraper: func(options scraper.ScraperOptions) (scraperHandle, error) { return scraper.NewScraper(options) },
		newProcessor: func(repo worker.Repo, acker worker.Acker, articleScraper worker.ArticleScraper, receiver worker.Receiver, options worker.Options) (processorHandle, error) {
			return worker.NewProcessor(repo, acker, articleScraper, receiver, options)
		},
		newPool: func(reader worker.QueueReader, processor worker.JobProcessor, options worker.PoolOptions) (poolHandle, error) {
			return worker.NewPool(reader, processor, options)
		},
		logger: slog.Default(),
	}
}

func runWithDependencies(parent context.Context, deps dependencies) (result error) {
	if parent == nil || invalid(deps.loadConfig) || invalid(deps.notifyContext) || invalid(deps.newDB) || invalid(deps.newConsumer) || invalid(deps.newScraper) || invalid(deps.newProcessor) || invalid(deps.newPool) {
		return coded("invalid_dependencies", nil)
	}
	logger := deps.logger
	if logger == nil {
		logger = slog.Default()
	}
	root, unregister := deps.notifyContext(parent, os.Interrupt, syscall.SIGTERM)
	var stopOnce sync.Once
	stopSignals := func() {
		stopOnce.Do(func() {
			if !invalid(unregister) {
				unregister()
			}
		})
	}
	defer stopSignals()
	if invalid(root) || invalid(unregister) {
		return coded("signal_context_unavailable", nil)
	}

	cfg, err := deps.loadConfig()
	if err != nil {
		return coded("configuration_failed", err)
	}
	if invalid(cfg) {
		return coded("configuration_unavailable", nil)
	}
	if err := root.Err(); err != nil {
		return coded("startup_canceled", err)
	}

	var database dbHandle
	var consumer consumerHandle
	defer func() {
		if consumer != nil {
			if err := consumer.Close(); err != nil {
				result = errors.Join(result, coded("consumer_close_failed", err))
			}
		}
		if database != nil {
			database.Close()
		}
	}()

	openCtx, cancelOpen := context.WithTimeout(root, cfg.DBTimeout)
	database, err = deps.newDB(openCtx, cfg.DatabaseURL)
	cancelOpen()
	if invalid(database) {
		database = nil
	}
	if err != nil {
		return coded("database_open_failed", err)
	}
	if database == nil {
		return coded("database_unavailable", nil)
	}
	if err := root.Err(); err != nil {
		return coded("startup_canceled", err)
	}
	pingCtx, cancelPing := context.WithTimeout(root, cfg.DBTimeout)
	err = database.Ping(pingCtx)
	cancelPing()
	if err != nil {
		return coded("database_ping_failed", err)
	}
	if err := root.Err(); err != nil {
		return coded("startup_canceled", err)
	}

	consumerOptions := queue.ConsumerOptions{RedisURL: cfg.RedisURL, Stream: cfg.RedisStream, Group: cfg.ConsumerGroup, Name: cfg.ConsumerName, Block: cfg.StreamBlock, OperationTimeout: cfg.QueueOperationTimeout, DialTimeout: cfg.RedisDialTimeout, PoolSize: cfg.PoolSize}
	consumer, err = deps.newConsumer(root, consumerOptions)
	if invalid(consumer) {
		consumer = nil
	}
	if err != nil {
		return coded("consumer_open_failed", err)
	}
	if consumer == nil {
		return coded("consumer_unavailable", nil)
	}
	groupCtx, cancelGroup := context.WithTimeout(root, cfg.QueueOperationTimeout)
	_, err = consumer.EnsureGroup(groupCtx)
	cancelGroup()
	if err != nil {
		return coded("consumer_group_failed", err)
	}
	if err := root.Err(); err != nil {
		return coded("startup_canceled", err)
	}

	scraperOptions := scraper.ScraperOptions{ConnectTimeout: cfg.ScraperConnectTimeout, TLSTimeout: cfg.ScraperTLSTimeout, HeaderTimeout: cfg.ScraperHeaderTimeout, ReadTimeout: cfg.ScraperReadTimeout, RequestTimeout: cfg.ScraperRequestTimeout, MaxBodyBytes: cfg.ScraperMaxBodyBytes, MaxRedirects: cfg.ScraperMaxRedirects}
	articleScraper, err := deps.newScraper(scraperOptions)
	if err != nil {
		return coded("scraper_init_failed", err)
	}
	if invalid(articleScraper) {
		return coded("scraper_unavailable", nil)
	}
	if err := root.Err(); err != nil {
		return coded("startup_canceled", err)
	}

	jobLogger := logger.With("consumer_group", cfg.ConsumerGroup, "consumer_name", cfg.ConsumerName)
	repo := db.NewRepository(database)
	processor, err := deps.newProcessor(repo, consumer, articleScraper, metadataReceiver{logger: jobLogger}, worker.Options{DBTimeout: cfg.DBTimeout, QueueOperationTimeout: cfg.QueueOperationTimeout, Logger: jobLogger})
	if err != nil {
		return coded("processor_init_failed", err)
	}
	if invalid(processor) {
		return coded("processor_unavailable", nil)
	}
	if err := root.Err(); err != nil {
		return coded("startup_canceled", err)
	}

	pool, err := deps.newPool(consumer, processor, worker.PoolOptions{PoolSize: cfg.PoolSize, ShutdownTimeout: cfg.ShutdownTimeout, BackoffDelay: time.Second, Logger: jobLogger})
	if err != nil {
		return coded("pool_init_failed", err)
	}
	if invalid(pool) {
		return coded("pool_unavailable", nil)
	}
	logger.Info("worker_started", "consumer_group", cfg.ConsumerGroup, "consumer_name", cfg.ConsumerName, "pool_size", cfg.PoolSize)
	if err := pool.Run(root); err != nil {
		return coded("worker_run_failed", err)
	}
	return nil
}

// coded errors expose stable text while preserving errors.Is/errors.As access to
// causes that may contain connection strings or payload data.
type codedError struct {
	code  string
	cause error
}

func (e *codedError) Error() string { return "worker: " + e.code }
func (e *codedError) Unwrap() error { return e.cause }
func coded(code string, cause error) error {
	if cause == nil {
		return &codedError{code: code}
	}
	return &codedError{code: code, cause: cause}
}

func invalid(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

type metadataReceiver struct{ logger *slog.Logger }

func (r metadataReceiver) Receive(ctx context.Context, result worker.Result) error {
	if ctx == nil {
		return coded("receiver_context_unavailable", nil)
	}
	if err := ctx.Err(); err != nil {
		return coded("receiver_canceled", err)
	}
	if result.JobID == uuid.Nil || (result.Article == nil) == (result.Error == nil) {
		return coded("invalid_receiver_result", nil)
	}
	logger := r.logger
	if logger == nil {
		logger = slog.Default()
	}
	if article := result.Article; article != nil {
		logger.InfoContext(ctx, "worker_result_received", "job_id", result.JobID.String(), "result", "article", "text_bytes", len(article.Text), "title_present", article.Title != "", "language_present", article.Language != "")
		return nil
	}
	e := result.Error
	logger.InfoContext(ctx, "worker_result_received", "job_id", result.JobID.String(), "result", "scrape_error", "kind", safeKind(e.Kind), "stage", safeStage(e.Stage), "http_status", safeStatus(e.HTTPStatus))
	return nil
}
func safeKind(kind scraper.ScrapeErrorKind) string {
	switch kind {
	case scraper.ScrapeErrorSecurityRejected, scraper.ScrapeErrorTimeout, scraper.ScrapeErrorNetwork, scraper.ScrapeErrorCanceled, scraper.ScrapeErrorBodyTooLarge, scraper.ScrapeErrorHTTPStatus, scraper.ScrapeErrorUnsupported, scraper.ScrapeErrorExtraction:
		return string(kind)
	}
	return "unknown"
}
func safeStage(stage scraper.ScrapeStage) string {
	switch stage {
	case scraper.ScrapeStageValidation, scraper.ScrapeStageFetch, scraper.ScrapeStageExtract:
		return string(stage)
	}
	return "unknown"
}
func safeStatus(status int) int {
	if status >= 100 && status <= 599 {
		return status
	}
	return 0
}
