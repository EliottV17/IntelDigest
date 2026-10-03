//go:build integration

package queue

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

func TestPublisherPublishesMessageToRedisStream(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not configured; skipping Redis integration test")
	}

	stream := "inteldigest:integration:" + uuid.NewString()
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	reader := redis.NewClient(options)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := reader.Del(ctx, stream).Err(); err != nil {
			t.Errorf("clean up Redis stream %q: %v", stream, err)
		}
		if err := reader.Close(); err != nil {
			t.Errorf("close Redis test client: %v", err)
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	publisher, err := NewPublisher(ctx, redisURL, stream, time.Second, time.Second)
	if err != nil {
		t.Fatalf("connect publisher to Redis: %v", err)
	}
	t.Cleanup(func() {
		if err := publisher.Close(); err != nil {
			t.Errorf("close publisher: %v", err)
		}
	})

	message := Message{
		SchemaVersion: SchemaVersion,
		JobID:         uuid.New(),
		URL:           "https://example.com/integration-test",
	}
	if err := publisher.Publish(ctx, message); err != nil {
		t.Fatalf("publish message: %v", err)
	}

	entries, err := reader.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("read Redis stream: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("stream contains %d entries, want exactly one", len(entries))
	}
	got, ok := entries[0].Values["data"].(string)
	if !ok {
		t.Fatalf("stream data has type %T, want string", entries[0].Values["data"])
	}
	want := fmt.Sprintf(`{"schema_version":1,"job_id":"%s","url":"%s"}`, message.JobID, message.URL)
	if got != want {
		t.Errorf("stream data = %q, want exact JSON payload %q", got, want)
	}
}
