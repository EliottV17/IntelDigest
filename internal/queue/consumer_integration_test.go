//go:build integration

package queue

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestConsumerIntegrationGroupDeliveryDistributionAndAck(t *testing.T) {
	admin, ctx, stream, group, redisURL := consumerIntegrationSetup(t)
	nameA, nameB := "queue-it-"+uuid.NewString(), "queue-it-"+uuid.NewString()
	first := newIntegrationConsumer(t, ctx, redisURL, stream, group, nameA, 400*time.Millisecond, time.Second)
	second := newIntegrationConsumer(t, ctx, redisURL, stream, group, nameB, 400*time.Millisecond, time.Second)

	preID, err := admin.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]interface{}{"data": "before-start"}}).Result()
	if err != nil {
		t.Fatal(err)
	}
	created, err := first.EnsureGroup(ctx)
	if err != nil || !created {
		t.Fatalf("first EnsureGroup = (%v, %v)", created, err)
	}
	created, err = second.EnsureGroup(ctx)
	if err != nil || created {
		t.Fatalf("existing EnsureGroup = (%v, %v)", created, err)
	}
	old, ok, err := first.Read(ctx)
	if err != nil || !ok || old.ID != preID {
		t.Fatalf("pre-start read = (%+v, %v, %v), want ID %s", old, ok, err, preID)
	}

	reads := make(chan readResult, 2)
	go func() { delivery, ok, err := first.Read(ctx); reads <- readResult{delivery, ok, err} }()
	go func() { delivery, ok, err := second.Read(ctx); reads <- readResult{delivery, ok, err} }()
	waitForBlockedReaders(t, admin, []string{nameA, nameB}, time.Second)
	for _, value := range []string{"one", "two"} {
		if _, err := admin.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]interface{}{"data": value}}).Result(); err != nil {
			t.Fatal(err)
		}
	}
	got := make(map[string]Delivery, 2)
	for range 2 {
		select {
		case read := <-reads:
			if read.err != nil || !read.ok {
				t.Fatalf("new-entry read = (%+v, %v, %v)", read.delivery, read.ok, read.err)
			}
			got[read.delivery.ID] = read.delivery
		case <-ctx.Done():
			t.Fatal("timed out waiting for both consumers")
		}
	}
	if len(got) != 2 {
		t.Fatalf("consumer deliveries = %d unique IDs, want 2", len(got))
	}
	if _, duplicate := got[old.ID]; duplicate {
		t.Fatalf("new read redelivered pre-start ID %s", old.ID)
	}

	var acknowledged Delivery
	for _, delivery := range got {
		acknowledged = delivery
		break
	}
	if err := first.Ack(ctx, acknowledged); err != nil {
		t.Fatal(err)
	}
	pending, err := admin.XPending(ctx, stream, group).Result()
	if err != nil {
		t.Fatal(err)
	}
	if pending.Count != 2 {
		t.Fatalf("pending count after one new ACK = %d, want 2", pending.Count)
	}
	if err := first.Ack(ctx, old); err != nil {
		t.Fatal(err)
	}
	pending, err = admin.XPending(ctx, stream, group).Result()
	if err != nil || pending.Count != 1 {
		t.Fatalf("pending after old-entry ACK = (%d, %v), want 1", pending.Count, err)
	}
}

func TestConsumerIntegrationBlockedReadCancellationAndReuse(t *testing.T) {
	admin, ctx, stream, group, redisURL := consumerIntegrationSetup(t)
	name := "queue-cancel-" + uuid.NewString()
	block, operation := 400*time.Millisecond, 500*time.Millisecond
	consumer := newIntegrationConsumer(t, ctx, redisURL, stream, group, name, block, operation)
	if created, err := consumer.EnsureGroup(ctx); err != nil || !created {
		t.Fatalf("EnsureGroup = (%v, %v)", created, err)
	}

	readCtx, cancelRead := context.WithCancel(ctx)
	started := make(chan struct{})
	result := make(chan readResult, 1)
	go func() {
		close(started)
		delivery, ok, err := consumer.Read(readCtx)
		result <- readResult{delivery, ok, err}
	}()
	<-started
	// The CLIENT LIST observation, not the goroutine-start channel, proves the
	// XREADGROUP request reached Redis and is actually blocked there.
	waitForBlockedReaders(t, admin, []string{name}, time.Second)
	cancelAt := time.Now()
	cancelRead()
	select {
	case read := <-result:
		if read.ok {
			t.Fatalf("canceled read delivered unexpectedly: %+v", read.delivery)
		}
		if read.err != nil && !errors.Is(read.err, context.Canceled) && !errors.Is(read.err, context.DeadlineExceeded) {
			var networkErr net.Error
			if !errors.As(read.err, &networkErr) || !networkErr.Timeout() {
				t.Fatalf("canceled read returned unrelated error: %v", read.err)
			}
		}
	case <-time.After(block + operation + 250*time.Millisecond):
		t.Fatal("canceled XREADGROUP did not return within its block-plus-operation bound")
	}
	if elapsed := time.Since(cancelAt); elapsed > block+operation+250*time.Millisecond {
		t.Fatalf("canceled read took %v beyond bounded budget", elapsed)
	}

	// The same client must remain usable. An ordinary empty BLOCK timeout is a
	// normal no-delivery result and must terminate within its finite budget.
	startedAt := time.Now()
	_, ok, err := consumer.Read(ctx)
	if err != nil || ok {
		t.Fatalf("normal empty read = (%v, %v), want no delivery and nil", ok, err)
	}
	if elapsed := time.Since(startedAt); elapsed > block+operation+250*time.Millisecond {
		t.Fatalf("normal block read exceeded budget: %v", elapsed)
	}
}

type readResult struct {
	delivery Delivery
	ok       bool
	err      error
}

func consumerIntegrationSetup(t *testing.T) (*redis.Client, context.Context, string, string, string) {
	t.Helper()
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is required for consumer integration tests")
	}
	parsed, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal("parse Redis URL")
	}
	admin := redis.NewClient(parsed)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	stream := "inteldigest:consumer-integration:" + uuid.NewString()
	group := "group:" + uuid.NewString()
	t.Cleanup(func() {
		defer cancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cleanupCancel()
		if err := admin.Del(cleanupCtx, stream).Err(); err != nil {
			t.Errorf("clean up isolated stream: %v", err)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("close integration Redis client: %v", err)
		}
	})
	if err := admin.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect to configured Redis: %v", err)
	}
	return admin, ctx, stream, group, redisURL
}

func newIntegrationConsumer(t *testing.T, ctx context.Context, redisURL, stream, group, name string, block, operation time.Duration) *Consumer {
	t.Helper()
	consumer, err := NewConsumer(ctx, ConsumerOptions{
		RedisURL: redisURL, Stream: stream, Group: group, Name: name,
		Block: block, OperationTimeout: operation, DialTimeout: time.Second, PoolSize: 2,
	})
	if err != nil {
		t.Fatalf("connect consumer: %v", err)
	}
	t.Cleanup(func() {
		if err := consumer.Close(); err != nil {
			t.Errorf("close consumer: %v", err)
		}
	})
	return consumer
}

func waitForBlockedReaders(t *testing.T, admin *redis.Client, names []string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		clients, err := admin.ClientList(ctx).Result()
		if err != nil {
			t.Fatalf("inspect Redis clients: %v", err)
		}
		allBlocked := true
		for _, name := range names {
			found := false
			for _, line := range strings.Split(clients, "\n") {
				if strings.Contains(line, "name="+name+" ") && strings.Contains(line, "cmd=xreadgroup") && strings.Contains(line, "flags=b") {
					found = true
					break
				}
			}
			if !found {
				allBlocked = false
				break
			}
		}
		if allBlocked {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Redis did not report all named readers blocked: %v", ctx.Err())
		case <-ticker.C:
		}
	}
}
