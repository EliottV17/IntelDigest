package worker

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"inteldigest/internal/queue"
)

type poolRead struct {
	delivery queue.Delivery
	ok       bool
	err      error
}

type poolReader struct {
	reads   chan poolRead
	started chan struct{}
	calls   atomic.Int32
	block   <-chan struct{}
}

func (r *poolReader) Read(ctx context.Context) (queue.Delivery, bool, error) {
	r.calls.Add(1)
	if r.started != nil {
		r.started <- struct{}{}
	}
	if r.block != nil {
		select {
		case <-r.block:
		case <-ctx.Done():
			return queue.Delivery{}, false, ctx.Err()
		}
	}
	select {
	case got := <-r.reads:
		return got.delivery, got.ok, got.err
	case <-ctx.Done():
		return queue.Delivery{}, false, ctx.Err()
	}
}

type poolCall struct {
	admission context.Context
	job       context.Context
	delivery  queue.Delivery
}

type poolProcessor struct {
	calls chan poolCall
	fn    func(poolCall) error
}

func (p *poolProcessor) Process(admission, job context.Context, d queue.Delivery) (Outcome, error) {
	call := poolCall{admission: admission, job: job, delivery: d}
	p.calls <- call
	if p.fn != nil {
		return OutcomeSucceeded, p.fn(call)
	}
	return OutcomeSucceeded, nil
}

type lateDeliveryReader struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (r *lateDeliveryReader) Read(ctx context.Context) (queue.Delivery, bool, error) {
	r.calls.Add(1)
	close(r.entered)
	<-r.release // deliberately returns a delivery after admission cancellation
	return queue.Delivery{ID: "late"}, true, nil
}

type longReader struct {
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (r *longReader) Read(ctx context.Context) (queue.Delivery, bool, error) {
	if r.calls.Add(1) == 1 {
		return queue.Delivery{ID: "confirmed"}, true, nil
	}
	close(r.entered)
	<-r.release // demonstrates drain is independent of the reader's finite budget
	return queue.Delivery{}, false, context.Canceled
}

func newPoolFakes(n int) (*poolReader, *poolProcessor) {
	return &poolReader{reads: make(chan poolRead, n), started: make(chan struct{}, n+8)}, &poolProcessor{calls: make(chan poolCall, n)}
}

func testPool(t *testing.T, reader QueueReader, processor JobProcessor, options PoolOptions) *Pool {
	t.Helper()
	p, err := NewPool(reader, processor, options)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	return p
}

func TestPoolOptionsAndConstructorValidation(t *testing.T) {
	defaults := DefaultPoolOptions()
	if defaults.PoolSize != 4 || defaults.ShutdownTimeout != 10*time.Second || defaults.BackoffDelay != time.Second || defaults.Logger == nil {
		t.Fatalf("unexpected defaults: %#v", defaults)
	}
	reader, processor := newPoolFakes(1)
	valid := defaults
	valid.ShutdownTimeout = time.Millisecond
	if _, err := NewPool(reader, processor, valid); err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
	cases := []struct {
		name string
		read QueueReader
		proc JobProcessor
		opts PoolOptions
	}{
		{"nil reader", nil, processor, valid},
		{"nil processor", reader, nil, valid},
		{"zero workers", reader, processor, func() PoolOptions { o := valid; o.PoolSize = 0; return o }()},
		{"too many workers", reader, processor, func() PoolOptions { o := valid; o.PoolSize = 65; return o }()},
		{"zero drain", reader, processor, func() PoolOptions { o := valid; o.ShutdownTimeout = 0; return o }()},
		{"zero backoff", reader, processor, func() PoolOptions { o := valid; o.BackoffDelay = 0; return o }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPool(tc.read, tc.proc, tc.opts); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	var typedNil *poolReader
	if _, err := NewPool(typedNil, processor, valid); err == nil {
		t.Fatal("typed nil reader accepted")
	}
}

func TestPoolFixedConcurrencyAndSequentialSlotReuse(t *testing.T) {
	const workers = 3
	reader, processor := newPoolFakes(8)
	entered := make(chan poolCall, workers)
	release := make(chan struct{})
	processor.fn = func(c poolCall) error { entered <- c; <-release; return nil }
	for i := 0; i < 8; i++ {
		reader.reads <- poolRead{delivery: queue.Delivery{ID: "d"}, ok: true}
	}
	options := DefaultPoolOptions()
	options.PoolSize = workers
	options.ShutdownTimeout = time.Second
	pool := testPool(t, reader, processor, options)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pool.Run(ctx) }()
	for i := 0; i < workers; i++ {
		<-entered
	}
	if got := reader.calls.Load(); got != workers {
		t.Fatalf("reads while all workers held = %d, want %d", got, workers)
	}
	for i := 0; i < 5; i++ {
		release <- struct{}{}
	}
	for i := 0; i < 5; i++ {
		<-entered
	}
	cancel()
	for i := 0; i < workers; i++ {
		release <- struct{}{}
	}
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := reader.calls.Load(); got != workers+5 {
		t.Fatalf("read count %d, want %d", got, workers+5)
	}
}

func TestPoolEmptyReadAndPerJobErrorContinueWithoutRetry(t *testing.T) {
	reader, processor := newPoolFakes(4)
	secret := "DO_NOT_LOG_JOB_ERROR"
	processor.fn = func(c poolCall) error {
		if c.delivery.ID == "bad" {
			return errors.New(secret)
		}
		return nil
	}
	reader.reads <- poolRead{ok: false}
	reader.reads <- poolRead{delivery: queue.Delivery{ID: "bad"}, ok: true}
	reader.reads <- poolRead{delivery: queue.Delivery{ID: "good"}, ok: true}
	ctx, cancel := context.WithCancel(context.Background())
	options := DefaultPoolOptions()
	options.PoolSize = 1
	var logs strings.Builder
	options.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	p := testPool(t, reader, processor, options)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	first, second := <-processor.calls, <-processor.calls
	if first.delivery.ID != "bad" || second.delivery.ID != "good" {
		t.Fatalf("processed %q then %q", first.delivery.ID, second.delivery.ID)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := reader.calls.Load(); got != 4 {
		t.Fatalf("read count %d, want empty+2 deliveries+stop read", got)
	}
	if strings.Contains(logs.String(), secret) {
		t.Fatal("raw process error leaked to logs")
	}
}

func TestPoolTransientReadErrorBackoffCanCancel(t *testing.T) {
	reader, processor := newPoolFakes(1)
	reader.reads <- poolRead{err: errors.New("SECRET_READ_ERROR")}
	ctx, cancel := context.WithCancel(context.Background())
	options := DefaultPoolOptions()
	options.PoolSize = 1
	options.BackoffDelay = time.Hour
	var logs strings.Builder
	options.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	p := testPool(t, reader, processor, options)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	<-reader.started
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if reader.calls.Load() != 1 {
		t.Fatalf("backoff hot-looped: reads=%d", reader.calls.Load())
	}
	if strings.Contains(logs.String(), "SECRET_READ_ERROR") {
		t.Fatal("raw read error leaked to logs")
	}
}

func TestPoolWrappedGroupMissingStopsAndPreservesCause(t *testing.T) {
	cause := &queue.GroupMissingError{}
	reader, processor := newPoolFakes(2)
	reader.reads <- poolRead{err: errors.Join(errors.New("outer private"), cause)}
	options := DefaultPoolOptions()
	options.PoolSize = 2
	var logs strings.Builder
	options.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	p := testPool(t, reader, processor, options)
	err := p.Run(context.Background())
	if !queue.IsGroupMissing(err) {
		t.Fatalf("fatal cause lost: %v", err)
	}
	if strings.Contains(err.Error(), "outer private") || strings.Contains(logs.String(), "outer private") {
		t.Fatal("unsafe fatal detail exposed")
	}
	if processorCount := len(processor.calls); processorCount != 0 {
		t.Fatalf("processed %d deliveries", processorCount)
	}
	if got := reader.calls.Load(); got < 1 || got > 2 {
		t.Fatalf("unexpected reader count after fatal stop: %d", got)
	}
}

func TestPoolJobContextPreservesValuesButNotRootDeadline(t *testing.T) {
	type key struct{}
	root, cancel := context.WithDeadline(context.WithValue(context.Background(), key{}, "root-value"), time.Now().Add(time.Hour))
	defer cancel()
	reader, processor := newPoolFakes(1)
	reader.reads <- poolRead{delivery: queue.Delivery{ID: "one"}, ok: true}
	options := DefaultPoolOptions()
	options.PoolSize = 1
	jobRelease := make(chan struct{})
	processor.fn = func(poolCall) error { <-jobRelease; return nil }
	p := testPool(t, reader, processor, options)
	ctx, stop := context.WithCancel(root)
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	call := <-processor.calls
	if call.job.Value(key{}) != "root-value" {
		t.Fatal("root value not preserved")
	}
	if _, ok := call.job.Deadline(); ok {
		t.Fatal("root deadline leaked into job context")
	}
	stop()
	select {
	case <-call.job.Done():
		t.Fatal("job canceled before drain expiry")
	default:
	}
	close(jobRelease)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPoolLateDeliveryAfterCancellationIsNotProcessed(t *testing.T) {
	reader := &lateDeliveryReader{entered: make(chan struct{}), release: make(chan struct{})}
	processor := &poolProcessor{calls: make(chan poolCall, 1)}
	options := DefaultPoolOptions()
	options.PoolSize = 1
	options.ShutdownTimeout = time.Second
	p := testPool(t, reader, processor, options)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	<-reader.entered
	cancel()
	close(reader.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(processor.calls) != 0 {
		t.Fatal("late delivery was claimed by processor")
	}
}

func TestPoolDrainTimerRunsWhileLongReaderAndConfirmedJobRemain(t *testing.T) {
	reader := &longReader{entered: make(chan struct{}), release: make(chan struct{})}
	jobEntered, releaseJob := make(chan poolCall, 1), make(chan struct{})
	processor := &poolProcessor{calls: make(chan poolCall, 1), fn: func(c poolCall) error { jobEntered <- c; <-releaseJob; return nil }}
	options := DefaultPoolOptions()
	options.PoolSize = 2
	options.ShutdownTimeout = 20 * time.Millisecond
	p := testPool(t, reader, processor, options)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	call := <-jobEntered
	<-reader.entered
	cancel()
	select {
	case <-call.job.Done():
	case <-time.After(time.Second):
		t.Fatal("drain timer did not cancel job while reader remained blocked")
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned before reader and processor joined: %v", err)
	default:
	}
	close(reader.release)
	close(releaseJob)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPoolCanceledContextAndSingleRunLifecycle(t *testing.T) {
	reader, processor := newPoolFakes(1)
	options := DefaultPoolOptions()
	options.PoolSize = 1
	p := testPool(t, reader, processor, options)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("already canceled Run: %v", err)
	}
	if reader.calls.Load() != 0 {
		t.Fatalf("already canceled context read %d times", reader.calls.Load())
	}
	if err := p.Run(context.Background()); err == nil {
		t.Fatal("second Run should be rejected")
	}
}
