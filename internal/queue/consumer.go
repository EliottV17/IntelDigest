package queue

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const busyGroupMessage = "BUSYGROUP Consumer Group name already exists"

// Delivery contains the Redis Stream entry identity and uninterpreted field values.
// Payload decoding and validation belong to the worker.
type Delivery struct {
	ID     string
	Fields map[string]interface{}
}

// ConsumerOptions describes the Redis connection and one stream consumer.
type ConsumerOptions struct {
	RedisURL         string
	Stream           string
	Group            string
	Name             string
	Block            time.Duration
	OperationTimeout time.Duration
	DialTimeout      time.Duration
	PoolSize         int
}

type consumerRedisClient interface {
	Do(context.Context, ...interface{}) *redis.Cmd
	XReadGroup(context.Context, *redis.XReadGroupArgs) *redis.XStreamSliceCmd
	Close() error
}

// Consumer owns its Redis client and issues only group-create, new-entry read,
// and acknowledgement commands. It never claims, replays, or deletes entries.
type Consumer struct {
	client           consumerRedisClient
	stream           string
	group            string
	name             string
	block            time.Duration
	operationTimeout time.Duration
	readTimeout      time.Duration
	closeOnce        sync.Once
	closeErr         error
}

// GroupMissingError marks Redis NOGROUP responses so callers can stop rather
// than silently recreate a group and change its position/PEL.
type GroupMissingError struct{ cause error }

func (err *GroupMissingError) Error() string { return "queue: Redis consumer group is missing" }
func (err *GroupMissingError) Unwrap() error { return err.cause }

// IsGroupMissing reports whether err carries Redis' NOGROUP response.
func IsGroupMissing(err error) bool {
	var missing *GroupMissingError
	return errors.As(err, &missing)
}

func normalizeBlock(block time.Duration) (time.Duration, error) {
	if block < time.Millisecond {
		return 0, errors.New("queue: read block must be at least 1ms")
	}
	// Redis accepts integer milliseconds; normalize the duration accepted by
	// config.WorkerConfig at the queue boundary.
	millis := block.Milliseconds()
	if millis < 1 {
		return 0, errors.New("queue: read block must be at least 1ms")
	}
	return time.Duration(millis) * time.Millisecond, nil
}

func validateConsumerOptions(options ConsumerOptions) (*redis.Options, error) {
	if options.Stream == "" || options.Group == "" || options.Name == "" || strings.TrimSpace(options.Group) == "" || strings.TrimSpace(options.Name) == "" {
		return nil, errors.New("queue: stream, group, and consumer name are required")
	}
	block, err := normalizeBlock(options.Block)
	if err != nil {
		return nil, err
	}
	if options.OperationTimeout <= 0 || options.DialTimeout <= 0 {
		return nil, errors.New("queue: operation and dial timeouts must be positive")
	}
	if int64(block) > math.MaxInt64-int64(options.OperationTimeout) {
		return nil, errors.New("queue: read timeout overflows")
	}
	if options.PoolSize <= 0 || options.PoolSize > math.MaxInt-2 {
		return nil, errors.New("queue: pool size must be positive and leave room for two control connections")
	}
	parsed, err := redis.ParseURL(options.RedisURL)
	if err != nil || parsed == nil {
		return nil, errors.New("queue: invalid Redis URL")
	}
	parsed.DialTimeout = options.DialTimeout
	parsed.ReadTimeout = block + options.OperationTimeout
	parsed.WriteTimeout = options.OperationTimeout
	parsed.PoolSize = options.PoolSize + 2
	parsed.MaxRetries = -1
	parsed.ContextTimeoutEnabled = true
	parsed.ClientName = options.Name
	return parsed, nil
}

// NewConsumer validates options, connects within DialTimeout, and owns the
// resulting concurrency-safe client. PoolSize represents reader capacity; two
// additional connections are reserved for control/ack work.
func NewConsumer(ctx context.Context, options ConsumerOptions) (*Consumer, error) {
	block, err := normalizeBlock(options.Block)
	if err != nil {
		return nil, err
	}
	options.Block = block
	redisOptions, err := validateConsumerOptions(options)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(redisOptions)
	pingCtx, cancel := context.WithTimeout(ctx, options.DialTimeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("queue: connect to Redis: %w", err)
	}
	return &Consumer{
		client:           client,
		stream:           options.Stream,
		group:            options.Group,
		name:             options.Name,
		block:            block,
		operationTimeout: options.OperationTimeout,
		readTimeout:      redisOptions.ReadTimeout,
	}, nil
}

// EnsureGroup creates the group at the beginning of the stream if absent.
// created is true only when this call created it; BUSYGROUP is idempotent.
func (consumer *Consumer) EnsureGroup(ctx context.Context) (created bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, consumer.operationTimeout)
	defer cancel()
	_, err = consumer.client.Do(ctx, "XGROUP", "CREATE", consumer.stream, consumer.group, "0", "MKSTREAM").Result()
	if err == nil {
		return true, nil
	}
	if isExactBusyGroup(err) {
		return false, nil
	}
	return false, fmt.Errorf("queue: ensure consumer group: %w", err)
}

func isExactBusyGroup(err error) bool {
	var redisErr redis.Error
	return errors.As(err, &redisErr) && redisErr.Error() == busyGroupMessage
}

// Read waits for at most the configured finite block plus operational margin.
// An empty block timeout is represented by (zero, false, nil). Cancellation is
// bounded by the context/socket deadline; go-redis does not immediately interrupt
// a blocked socket on cancellation alone.
func (consumer *Consumer) Read(ctx context.Context) (Delivery, bool, error) {
	readCtx, cancel := context.WithTimeout(ctx, consumer.readTimeout)
	defer cancel()
	streams, err := consumer.client.XReadGroup(readCtx, &redis.XReadGroupArgs{
		Group:    consumer.group,
		Consumer: consumer.name,
		Streams:  []string{consumer.stream, ">"},
		Count:    1,
		Block:    consumer.block,
		NoAck:    false,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return Delivery{}, false, nil
	}
	if err != nil {
		if redisHasPrefix(err, "NOGROUP") {
			return Delivery{}, false, &GroupMissingError{cause: err}
		}
		return Delivery{}, false, fmt.Errorf("queue: read stream: %w", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return Delivery{}, false, nil
	}
	message := streams[0].Messages[0]
	return Delivery{ID: message.ID, Fields: message.Values}, true, nil
}

func redisHasPrefix(err error, prefix string) bool {
	var redisErr redis.Error
	return errors.As(err, &redisErr) && strings.HasPrefix(redisErr.Error(), prefix+" ")
}

// Ack acknowledges exactly the Redis Stream entry ID; it does not delete it.
func (consumer *Consumer) Ack(ctx context.Context, delivery Delivery) error {
	ctx, cancel := context.WithTimeout(ctx, consumer.operationTimeout)
	defer cancel()
	_, err := consumer.client.Do(ctx, "XACK", consumer.stream, consumer.group, delivery.ID).Result()
	if err != nil {
		return fmt.Errorf("queue: acknowledge stream entry: %w", err)
	}
	return nil
}

// Close releases the owned Redis client and is safe to call repeatedly.
func (consumer *Consumer) Close() error {
	if consumer == nil || consumer.client == nil {
		return nil
	}
	consumer.closeOnce.Do(func() { consumer.closeErr = consumer.client.Close() })
	if consumer.closeErr != nil {
		return fmt.Errorf("queue: close Redis client: %w", consumer.closeErr)
	}
	return nil
}
