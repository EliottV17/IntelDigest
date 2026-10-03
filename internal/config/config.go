// Package config loads application settings from environment variables.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

const (
	defaultRedisStream         = "inteldigest:jobs"
	defaultRedisDialTimeout    = 5 * time.Second
	defaultRedisPublishTimeout = 2 * time.Second
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

// Load reads configuration from environment variables.
// DATABASE_URL and REDIS_URL are required; API_PORT, Redis stream, and
// Redis operation timeouts have application defaults.
func Load() (*Config, error) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return nil, errors.New("config: DATABASE_URL is required")
	}

	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return nil, errors.New("config: REDIS_URL is required")
	}
	if err := validateRedisURL(redisURL); err != nil {
		return nil, fmt.Errorf("config: invalid REDIS_URL: %w", err)
	}

	redisStream := os.Getenv("REDIS_STREAM")
	if redisStream == "" {
		redisStream = defaultRedisStream
	}

	dialTimeout, err := durationEnv("REDIS_DIAL_TIMEOUT", defaultRedisDialTimeout)
	if err != nil {
		return nil, err
	}
	publishTimeout, err := durationEnv("REDIS_PUBLISH_TIMEOUT", defaultRedisPublishTimeout)
	if err != nil {
		return nil, err
	}

	return &Config{
		DatabaseURL:         dbURL,
		APIPort:             port,
		RedisURL:            redisURL,
		RedisStream:         redisStream,
		RedisDialTimeout:    dialTimeout,
		RedisPublishTimeout: publishTimeout,
	}, nil
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
