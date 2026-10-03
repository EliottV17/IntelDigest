package config_test

import (
	"testing"
	"time"

	"inteldigest/internal/config"
)

func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error when DATABASE_URL is empty")
	}
}

func TestLoadRequiresRedisURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	t.Setenv("REDIS_URL", "")

	_, err := config.Load()
	if err == nil {
		t.Fatal("expected error when REDIS_URL is empty")
	}
}

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	t.Setenv("API_PORT", "")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("REDIS_STREAM", "")
	t.Setenv("REDIS_DIAL_TIMEOUT", "")
	t.Setenv("REDIS_PUBLISH_TIMEOUT", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DatabaseURL != "postgres://u:p@localhost/db" {
		t.Errorf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.APIPort != "8080" {
		t.Errorf("expected default APIPort 8080, got %q", cfg.APIPort)
	}
	if cfg.RedisURL != "redis://localhost:6379/0" {
		t.Errorf("RedisURL = %q", cfg.RedisURL)
	}
	if cfg.RedisStream != "inteldigest:jobs" {
		t.Errorf("RedisStream = %q, want inteldigest:jobs", cfg.RedisStream)
	}
	if cfg.RedisDialTimeout != 5*time.Second {
		t.Errorf("RedisDialTimeout = %s, want 5s", cfg.RedisDialTimeout)
	}
	if cfg.RedisPublishTimeout != 2*time.Second {
		t.Errorf("RedisPublishTimeout = %s, want 2s", cfg.RedisPublishTimeout)
	}
}

func TestLoadCustomPort(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	t.Setenv("API_PORT", "3000")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.APIPort != "3000" {
		t.Errorf("APIPort = %q", cfg.APIPort)
	}
}

func TestLoadRedisSettings(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	t.Setenv("REDIS_URL", "rediss://cache.example:6380/2")
	t.Setenv("REDIS_STREAM", "jobs:test")
	t.Setenv("REDIS_DIAL_TIMEOUT", "1500ms")
	t.Setenv("REDIS_PUBLISH_TIMEOUT", "750ms")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.RedisURL != "rediss://cache.example:6380/2" || cfg.RedisStream != "jobs:test" {
		t.Errorf("Redis settings = (%q, %q)", cfg.RedisURL, cfg.RedisStream)
	}
	if cfg.RedisDialTimeout != 1500*time.Millisecond || cfg.RedisPublishTimeout != 750*time.Millisecond {
		t.Errorf("Redis timeouts = (%s, %s)", cfg.RedisDialTimeout, cfg.RedisPublishTimeout)
	}
}

func TestLoadRejectsInvalidRedisSettings(t *testing.T) {
	for _, test := range []struct {
		name, redisURL, dialTimeout, publishTimeout string
	}{
		{name: "URL", redisURL: "http://localhost:6379"},
		{name: "dial timeout", redisURL: "redis://localhost:6379/0", dialTimeout: "0s"},
		{name: "publish timeout", redisURL: "redis://localhost:6379/0", publishTimeout: "not-a-duration"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
			t.Setenv("REDIS_URL", test.redisURL)
			t.Setenv("REDIS_DIAL_TIMEOUT", test.dialTimeout)
			t.Setenv("REDIS_PUBLISH_TIMEOUT", test.publishTimeout)
			if _, err := config.Load(); err == nil {
				t.Fatal("expected invalid Redis setting to fail")
			}
		})
	}
}
