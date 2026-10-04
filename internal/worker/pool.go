package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"inteldigest/internal/queue"
)

// QueueReader supplies one new delivery at a time. An empty result represents a
// normal finite blocking-read timeout, not an error.
type QueueReader interface {
	Read(context.Context) (queue.Delivery, bool, error)
}

// JobProcessor processes a delivery with separate admission and job lifetimes.
type JobProcessor interface {
	Process(admissionCtx, jobCtx context.Context, delivery queue.Delivery) (Outcome, error)
}

// PoolOptions bounds concurrency and shutdown. BackoffDelay applies only to
// transient read errors; clean empty reads are governed by the reader.
type PoolOptions struct {
	PoolSize        int
	ShutdownTimeout time.Duration
	BackoffDelay    time.Duration
	Logger          *slog.Logger
}

// DefaultPoolOptions matches the worker's configured default concurrency and
// the fixed transient-read backoff.
func DefaultPoolOptions() PoolOptions {
	return PoolOptions{PoolSize: 4, ShutdownTimeout: 10 * time.Second, BackoffDelay: time.Second, Logger: slog.Default()}
}

// Pool owns worker goroutines only. Its reader and processor, and any services
// they use, remain caller-owned and must outlive Run.
type Pool struct {
	reader    QueueReader
	processor JobProcessor
	options   PoolOptions
	run       atomic.Bool
}

// NewPool validates dependencies and bounded worker options without creating
// or taking ownership of external resources.
func NewPool(reader QueueReader, processor JobProcessor, options PoolOptions) (*Pool, error) {
	if isNilDependency(reader) || isNilDependency(processor) {
		return nil, errors.New("worker pool dependencies are required")
	}
	if options.PoolSize < 1 || options.PoolSize > 64 {
		return nil, errors.New("worker pool size must be between 1 and 64")
	}
	if options.ShutdownTimeout <= 0 || options.BackoffDelay <= 0 {
		return nil, errors.New("worker pool shutdown timeout and backoff must be positive")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Pool{reader: reader, processor: processor, options: options}, nil
}

type groupMissingFatal struct{ cause error }

func (e *groupMissingFatal) Error() string { return "worker pool stopped: consumer group is missing" }
func (e *groupMissingFatal) Unwrap() error { return e.cause }

// Run starts exactly PoolSize persistent sequential workers and joins all of
// them before returning. It never closes caller-owned resources. Once root
// cancellation or a fatal missing-group response closes admission, confirmed
// jobs drain for ShutdownTimeout before their independent contexts are canceled.
func (p *Pool) Run(ctx context.Context) error {
	if p == nil || ctx == nil {
		return errors.New("worker pool and context are required")
	}
	if !p.run.CompareAndSwap(false, true) {
		return errors.New("worker pool can only be run once")
	}
	admission, stopAdmission := context.WithCancel(ctx)
	jobBase := context.WithoutCancel(ctx)
	jobCtx, cancelJobs := context.WithCancel(jobBase)
	defer cancelJobs()

	stopping := make(chan struct{})
	workersDone := make(chan struct{})
	var stopOnce sync.Once
	var fatalMu sync.Mutex
	var fatal error
	stop := func(err error) {
		if err != nil {
			fatalMu.Lock()
			if fatal == nil {
				fatal = &groupMissingFatal{cause: err}
			}
			fatalMu.Unlock()
		}
		stopOnce.Do(func() { stopAdmission(); close(stopping) })
	}

	rootWatcherDone := make(chan struct{})
	go func() {
		defer close(rootWatcherDone)
		select {
		case <-ctx.Done():
			stop(nil)
		case <-stopping:
		}
	}()
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-stopping:
			timer := time.NewTimer(p.options.ShutdownTimeout)
			defer timer.Stop()
			select {
			case <-timer.C:
				cancelJobs()
			case <-workersDone:
			}
		case <-workersDone:
		}
	}()

	var workers sync.WaitGroup
	workers.Add(p.options.PoolSize)
	for i := 0; i < p.options.PoolSize; i++ {
		go func() {
			defer workers.Done()
			for admission.Err() == nil {
				delivery, ok, err := p.reader.Read(admission)
				if err != nil {
					if queue.IsGroupMissing(err) {
						stop(err)
						return
					}
					if admission.Err() != nil {
						return
					}
					p.options.Logger.Warn("worker_pool_read_failed", "code", "transient")
					timer := time.NewTimer(p.options.BackoffDelay)
					select {
					case <-timer.C:
					case <-admission.Done():
						timer.Stop()
						return
					}
					continue
				}
				if admission.Err() != nil {
					return
				}
				if !ok {
					continue
				}
				_, processErr := p.processor.Process(admission, jobCtx, delivery)
				if processErr != nil {
					p.options.Logger.Warn("worker_pool_job_failed", "code", "processing")
				}
			}
		}()
	}
	workers.Wait()
	close(workersDone)
	stopAdmission()
	cancelJobs()
	<-rootWatcherDone
	<-shutdownDone
	fatalMu.Lock()
	defer fatalMu.Unlock()
	return fatal
}
