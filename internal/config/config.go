// Package config loads application settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"math"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	defaultRedisStream         = "inteldigest:jobs"
	defaultRedisDialTimeout    = 5 * time.Second
	defaultRedisPublishTimeout = 2 * time.Second

	defaultWorkerPoolSize         = 4
	defaultWorkerConsumerGroup    = "inteldigest:workers"
	defaultWorkerStreamBlock      = 2 * time.Second
	defaultWorkerOperationTimeout = 5 * time.Second
	defaultWorkerDBTimeout        = 5 * time.Second
	defaultWorkerShutdownTimeout  = 10 * time.Second
	defaultScraperConnectTimeout  = 5 * time.Second
	defaultScraperTLSTimeout      = 5 * time.Second
	defaultScraperHeaderTimeout   = 10 * time.Second
	defaultScraperReadTimeout     = 10 * time.Second
	defaultScraperRequestTimeout  = 30 * time.Second
	defaultScraperMaxBodyBytes    = int64(5_242_880)
	defaultScraperMaxRedirects    = 5
	maxWorkerPoolSize             = 64
	maxScraperRedirects           = 10
)

// Config holds all runtime configuration.
type Config struct {
	DatabaseURL         string
	APIPort             string
	RedisURL            string
	RedisStream         string
	RedisDialTimeout    time.Duration
	RedisPublishTimeout time.Duration
}

// Load reads API and shared connection settings. Worker-only variables are
// intentionally ignored so unrelated worker configuration cannot block the API.
func Load() (*Config, error) {
	cfg, err := loadCommonConfig()
	if err != nil {
		return nil, err
	}

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}
	publishTimeout, err := durationEnv("REDIS_PUBLISH_TIMEOUT", defaultRedisPublishTimeout)
	if err != nil {
		return nil, err
	}
	cfg.APIPort = port
	cfg.RedisPublishTimeout = publishTimeout
	return cfg, nil
}

// WorkerConfig combines shared database/Redis connection settings with
// validated worker and scraper limits. A consumer name supplied explicitly
// must be unique among simultaneously live worker processes.
type WorkerConfig struct {
	Config
	PoolSize                int
	ConsumerGroup           string
	ConsumerName            string
	StreamBlock             time.Duration
	QueueOperationTimeout   time.Duration
	QueueReadTimeout        time.Duration
	DBTimeout               time.Duration
	ShutdownTimeout         time.Duration
	ScraperConnectTimeout   time.Duration
	ScraperTLSTimeout       time.Duration
	ScraperHeaderTimeout    time.Duration
	ScraperReadTimeout      time.Duration
	ScraperRequestTimeout   time.Duration
	ScraperMaxBodyBytes     int64
	ScraperMaxBodyReadBytes int64
	ScraperMaxRedirects     int
}

// LoadWorker reads only shared connection settings and worker/scraper options;
// it does not require unrelated API settings. The scraper request timeout is a
// single global deadline (also bounded by the parent context), so stage and hop
// budgets are truncated to remaining time rather than summed during validation.
func LoadWorker() (*WorkerConfig, error) {
	common, err := loadCommonConfig()
	if err != nil {
		return nil, err
	}

	poolSize, err := boundedIntEnv("WORKER_POOL_SIZE", defaultWorkerPoolSize, 1, maxWorkerPoolSize)
	if err != nil {
		return nil, err
	}
	group, groupSet := os.LookupEnv("WORKER_CONSUMER_GROUP")
	if !groupSet {
		group = defaultWorkerConsumerGroup
	} else if strings.TrimSpace(group) == "" {
		return nil, errors.New("config: WORKER_CONSUMER_GROUP must be nonblank when set")
	}
	group = strings.TrimSpace(group)
	consumerName, nameSet := os.LookupEnv("WORKER_CONSUMER_NAME")
	if nameSet {
		if strings.TrimSpace(consumerName) == "" {
			return nil, errors.New("config: WORKER_CONSUMER_NAME must be nonblank when set")
		}
		consumerName = strings.TrimSpace(consumerName)
	} else {
		consumerName, err = generatedConsumerName()
		if err != nil {
			return nil, err
		}
	}

	block, err := durationMinimumEnv("WORKER_STREAM_BLOCK", defaultWorkerStreamBlock, time.Millisecond)
	if err != nil {
		return nil, err
	}
	operationTimeout, err := durationEnv("WORKER_QUEUE_OPERATION_TIMEOUT", defaultWorkerOperationTimeout)
	if err != nil {
		return nil, err
	}
	if int64(block) > math.MaxInt64-int64(operationTimeout) {
		return nil, errors.New("config: WORKER_STREAM_BLOCK plus WORKER_QUEUE_OPERATION_TIMEOUT overflows")
	}
	readTimeout := block + operationTimeout

	durationSettings := [...]struct {
		name string
		def  time.Duration
	}{
		{"WORKER_DB_TIMEOUT", defaultWorkerDBTimeout},
		{"WORKER_SHUTDOWN_TIMEOUT", defaultWorkerShutdownTimeout},
		{"SCRAPER_CONNECT_TIMEOUT", defaultScraperConnectTimeout},
		{"SCRAPER_TLS_TIMEOUT", defaultScraperTLSTimeout},
		{"SCRAPER_HEADER_TIMEOUT", defaultScraperHeaderTimeout},
		{"SCRAPER_READ_TIMEOUT", defaultScraperReadTimeout},
		{"SCRAPER_REQUEST_TIMEOUT", defaultScraperRequestTimeout},
	}
	var durations [len(durationSettings)]time.Duration
	for index, setting := range durationSettings {
		durations[index], err = durationEnv(setting.name, setting.def)
		if err != nil {
			return nil, err
		}
	}
	bodyBytes, err := positiveInt64Env("SCRAPER_MAX_BODY_BYTES", defaultScraperMaxBodyBytes)
	if err != nil {
		return nil, err
	}
	maxInt := int64(^uint(0) >> 1)
	if bodyBytes >= maxInt {
		return nil, errors.New("config: SCRAPER_MAX_BODY_BYTES must allow a safe max+1 read and int conversion")
	}
	redirects, err := boundedIntEnv("SCRAPER_MAX_REDIRECTS", defaultScraperMaxRedirects, 0, maxScraperRedirects)
	if err != nil {
		return nil, err
	}

	return &WorkerConfig{
		Config: *common, PoolSize: poolSize, ConsumerGroup: group, ConsumerName: consumerName,
		StreamBlock: block, QueueOperationTimeout: operationTimeout, QueueReadTimeout: readTimeout,
		DBTimeout: durations[0], ShutdownTimeout: durations[1],
		ScraperConnectTimeout: durations[2], ScraperTLSTimeout: durations[3],
		ScraperHeaderTimeout: durations[4], ScraperReadTimeout: durations[5],
		ScraperRequestTimeout: durations[6], ScraperMaxBodyBytes: bodyBytes,
		ScraperMaxBodyReadBytes: bodyBytes + 1, ScraperMaxRedirects: redirects,
	}, nil
}

func loadCommonConfig() (*Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("config: DATABASE_URL is required")
	}
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return nil, errors.New("config: REDIS_URL is required")
	}
	if err := validateRedisURL(redisURL); err != nil {
		return nil, fmt.Errorf("config: invalid REDIS_URL: %w", err)
	}
	stream := envDefault("REDIS_STREAM", defaultRedisStream)
	dialTimeout, err := durationEnv("REDIS_DIAL_TIMEOUT", defaultRedisDialTimeout)
	if err != nil {
		return nil, err
	}
	return &Config{DatabaseURL: dbURL, RedisURL: redisURL, RedisStream: stream, RedisDialTimeout: dialTimeout}, nil
}

func envDefault(name, defaultValue string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return defaultValue
}

func generatedConsumerName() (string, error) {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "", errors.New("config: could not determine hostname for worker consumer name")
	}
	return hostname + "-" + uuid.NewString(), nil
}

func boundedIntEnv(name string, defaultValue, minimum, maximum int) (int, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("config: %s must be an integer from %d through %d", name, minimum, maximum)
	}
	return parsed, nil
}

func positiveInt64Env(name string, defaultValue int64) (int64, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("config: %s must be a positive integer", name)
	}
	return parsed, nil
}

func durationMinimumEnv(name string, defaultValue, minimum time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < minimum {
		return 0, fmt.Errorf("config: %s must be at least %s", name, minimum)
	}
	return parsed, nil
}

func durationEnv(name string, defaultValue time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("config: %s must be a positive duration", name)
	}
	return duration, nil
}

func validateRedisURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "redis" && parsed.Scheme != "rediss") || parsed.Hostname() == "" {
		return errors.New("expected redis:// or rediss:// URL with a host")
	}
	if parsed.Path != "" && parsed.Path != "/" {
		database, err := strconv.Atoi(parsed.Path[1:])
		if err != nil || database < 0 {
			return errors.New("database path must be a non-negative integer")
		}
	}
	return nil
}
