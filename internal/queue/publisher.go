package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisStreamClient interface {
	Ping(context.Context) *redis.StatusCmd
	XAdd(context.Context, *redis.XAddArgs) *redis.StringCmd
	Close() error
}

// Publisher appends versioned job messages to a Redis Stream.
type Publisher struct {
	client         redisStreamClient
	stream         string
	publishTimeout time.Duration
}

// NewPublisher validates Redis configuration, creates a client, and confirms
// connectivity within dialTimeout. The returned publisher owns its client.
func NewPublisher(ctx context.Context, redisURL, stream string, dialTimeout, publishTimeout time.Duration) (*Publisher, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil || options == nil {
		return nil, errors.New("queue: invalid redis URL")
	}
	if stream == "" {
		return nil, errors.New("queue: Redis stream is required")
	}
	if dialTimeout <= 0 || publishTimeout <= 0 {
		return nil, errors.New("queue: dial and publish timeouts must be positive")
	}
	options.DialTimeout = dialTimeout

	client := redis.NewClient(options)
	pingCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("queue: connect to Redis: %w", err)
	}

	return &Publisher{client: client, stream: stream, publishTimeout: publishTimeout}, nil
}

// Publish serializes and appends a single job contract message to the stream.
func (publisher *Publisher) Publish(ctx context.Context, message Message) error {
	if err := message.Validate(); err != nil {
		return fmt.Errorf("queue: validate message: %w", err)
	}
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("queue: encode message: %w", err)
	}

	publishCtx, cancel := context.WithTimeout(ctx, publisher.publishTimeout)
	defer cancel()
	if err := publisher.client.XAdd(publishCtx, &redis.XAddArgs{
		Stream: publisher.stream,
		Values: map[string]interface{}{"data": string(data)},
	}).Err(); err != nil {
		return fmt.Errorf("queue: publish to stream %q: %w", publisher.stream, err)
	}
	return nil
}

// Close releases the Redis client resources.
func (publisher *Publisher) Close() error {
	if publisher == nil || publisher.client == nil {
		return nil
	}
	if err := publisher.client.Close(); err != nil {
		return fmt.Errorf("queue: close Redis client: %w", err)
	}
	return nil
}
