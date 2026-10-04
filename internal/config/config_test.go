package config_test

import (
	"os"
	"strings"
	"testing"
	"time"

	"inteldigest/internal/config"

	"github.com/google/uuid"
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

func TestLoadWorkerRejectsExplicitEmptySettings(t *testing.T) {
	for _, name := range []string{"WORKER_CONSUMER_NAME", "WORKER_CONSUMER_GROUP"} {
		t.Run(name, func(t *testing.T) {
			setWorkerEnv(t, map[string]string{})
			t.Setenv(name, "")
			if _, err := config.LoadWorker(); err == nil {
				t.Fatalf("LoadWorker() accepted explicitly empty %s", name)
			}
		})
	}
}

func TestLoadWorkerDefaults(t *testing.T) {
	setWorkerEnv(t, map[string]string{})
	cfg, err := config.LoadWorker()
	if err != nil {
		t.Fatalf("LoadWorker() error = %v", err)
	}
	if cfg.DatabaseURL != "postgres://u:p@localhost/db" || cfg.RedisURL != "redis://localhost:6379/0" || cfg.RedisStream != "inteldigest:jobs" || cfg.RedisDialTimeout != 5*time.Second {
		t.Errorf("shared connection config = %+v", cfg.Config)
	}
	if cfg.PoolSize != 4 || cfg.ConsumerGroup != "inteldigest:workers" || cfg.StreamBlock != 2*time.Second || cfg.QueueOperationTimeout != 5*time.Second || cfg.QueueReadTimeout != 7*time.Second || cfg.DBTimeout != 5*time.Second || cfg.ShutdownTimeout != 10*time.Second {
		t.Errorf("worker defaults = %+v", cfg)
	}
	if cfg.ScraperConnectTimeout != 5*time.Second || cfg.ScraperTLSTimeout != 5*time.Second || cfg.ScraperHeaderTimeout != 10*time.Second || cfg.ScraperReadTimeout != 10*time.Second || cfg.ScraperRequestTimeout != 30*time.Second || cfg.ScraperMaxBodyBytes != 5_242_880 || cfg.ScraperMaxBodyReadBytes != 5_242_881 || cfg.ScraperMaxRedirects != 5 {
		t.Errorf("scraper defaults = %+v", cfg)
	}
}

func TestLoadWorkerExplicitValues(t *testing.T) {
	setWorkerEnv(t, map[string]string{
		"WORKER_POOL_SIZE": "64", "WORKER_CONSUMER_GROUP": "workers:test", "WORKER_CONSUMER_NAME": "operator-1",
		"WORKER_STREAM_BLOCK": "1ms", "WORKER_QUEUE_OPERATION_TIMEOUT": "7s", "WORKER_DB_TIMEOUT": "3s", "WORKER_SHUTDOWN_TIMEOUT": "11s",
		"SCRAPER_CONNECT_TIMEOUT": "1s", "SCRAPER_TLS_TIMEOUT": "2s", "SCRAPER_HEADER_TIMEOUT": "3s", "SCRAPER_READ_TIMEOUT": "4s", "SCRAPER_REQUEST_TIMEOUT": "30s",
		"SCRAPER_MAX_BODY_BYTES": "1", "SCRAPER_MAX_REDIRECTS": "0",
	})
	cfg, err := config.LoadWorker()
	if err != nil {
		t.Fatalf("LoadWorker() error = %v", err)
	}
	if cfg.PoolSize != 64 || cfg.ConsumerGroup != "workers:test" || cfg.ConsumerName != "operator-1" || cfg.StreamBlock != time.Millisecond || cfg.QueueOperationTimeout != 7*time.Second || cfg.QueueReadTimeout != 7*time.Second+time.Millisecond || cfg.DBTimeout != 3*time.Second || cfg.ShutdownTimeout != 11*time.Second {
		t.Errorf("worker values = %+v", cfg)
	}
	if cfg.ScraperConnectTimeout != time.Second || cfg.ScraperTLSTimeout != 2*time.Second || cfg.ScraperHeaderTimeout != 3*time.Second || cfg.ScraperReadTimeout != 4*time.Second || cfg.ScraperRequestTimeout != 30*time.Second || cfg.ScraperMaxBodyBytes != 1 || cfg.ScraperMaxBodyReadBytes != 2 || cfg.ScraperMaxRedirects != 0 {
		t.Errorf("scraper values = %+v", cfg)
	}
}

func TestLoadWorkerAcceptsMinimumPoolSize(t *testing.T) {
	setWorkerEnv(t, map[string]string{"WORKER_POOL_SIZE": "1"})
	cfg, err := config.LoadWorker()
	if err != nil {
		t.Fatalf("LoadWorker() rejected minimum pool size: %v", err)
	}
	if cfg.PoolSize != 1 {
		t.Errorf("PoolSize = %d, want 1", cfg.PoolSize)
	}
}

func TestLoadWorkerRejectsCredentialLeakInErrors(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:db-secret@db.example/app")
	t.Setenv("REDIS_URL", "redis://user:redis-secret@")
	_, err := config.LoadWorker()
	if err == nil {
		t.Fatal("LoadWorker() accepted malformed Redis URL")
	}
	if strings.Contains(err.Error(), "db-secret") || strings.Contains(err.Error(), "redis-secret") {
		t.Fatalf("configuration error exposed connection credentials: %v", err)
	}
}

func TestLoadWorkerAutogeneratesUniqueConsumerNames(t *testing.T) {
	setWorkerEnv(t, map[string]string{})
	first, err := config.LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	second, err := config.LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{first.ConsumerName, second.ConsumerName} {
		if !strings.HasPrefix(name, hostname+"-") {
			t.Errorf("generated consumer name %q does not include hostname %q", name, hostname)
			continue
		}
		if _, err := uuid.Parse(strings.TrimPrefix(name, hostname+"-")); err != nil {
			t.Errorf("generated consumer name %q does not end in a UUID: %v", name, err)
		}
	}
	if first.ConsumerName == second.ConsumerName {
		t.Fatalf("generated consumer names must be unique: %q", first.ConsumerName)
	}
}

func TestLoadWorkerDoesNotSumScraperStagesAcrossRedirects(t *testing.T) {
	setWorkerEnv(t, map[string]string{
		"SCRAPER_CONNECT_TIMEOUT": "10s", "SCRAPER_TLS_TIMEOUT": "10s", "SCRAPER_HEADER_TIMEOUT": "10s",
		"SCRAPER_READ_TIMEOUT": "10s", "SCRAPER_REQUEST_TIMEOUT": "1s", "SCRAPER_MAX_REDIRECTS": "10",
	})
	if _, err := config.LoadWorker(); err != nil {
		t.Fatalf("stage and redirect budgets are truncated by the global request deadline, not summed: %v", err)
	}
}

func TestLoadWorkerRejectsInvalidSettings(t *testing.T) {
	for _, test := range []struct{ name, key, value string }{
		{"pool zero", "WORKER_POOL_SIZE", "0"}, {"pool negative", "WORKER_POOL_SIZE", "-1"}, {"pool too large", "WORKER_POOL_SIZE", "65"}, {"pool malformed", "WORKER_POOL_SIZE", "many"},
		{"blank group", "WORKER_CONSUMER_GROUP", "   "}, {"blank name", "WORKER_CONSUMER_NAME", " \t"},
		{"block zero", "WORKER_STREAM_BLOCK", "0s"}, {"block below minimum", "WORKER_STREAM_BLOCK", "1ns"}, {"block malformed", "WORKER_STREAM_BLOCK", "nope"},
		{"queue timeout zero", "WORKER_QUEUE_OPERATION_TIMEOUT", "0s"}, {"db timeout negative", "WORKER_DB_TIMEOUT", "-1s"}, {"shutdown malformed", "WORKER_SHUTDOWN_TIMEOUT", "nope"},
		{"connect zero", "SCRAPER_CONNECT_TIMEOUT", "0s"}, {"tls negative", "SCRAPER_TLS_TIMEOUT", "-1s"}, {"header malformed", "SCRAPER_HEADER_TIMEOUT", "nope"}, {"read zero", "SCRAPER_READ_TIMEOUT", "0s"}, {"request negative", "SCRAPER_REQUEST_TIMEOUT", "-1s"},
		{"body zero", "SCRAPER_MAX_BODY_BYTES", "0"}, {"body malformed", "SCRAPER_MAX_BODY_BYTES", "many"}, {"body plus one overflow", "SCRAPER_MAX_BODY_BYTES", "9223372036854775807"},
		{"redirects negative", "SCRAPER_MAX_REDIRECTS", "-1"}, {"redirects too large", "SCRAPER_MAX_REDIRECTS", "11"}, {"redirects malformed", "SCRAPER_MAX_REDIRECTS", "many"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setWorkerEnv(t, map[string]string{test.key: test.value})
			if _, err := config.LoadWorker(); err == nil {
				t.Fatalf("LoadWorker() accepted %s=%q", test.key, test.value)
			}
		})
	}
}

func TestLoadWorkerRejectsTimeoutAdditionOverflow(t *testing.T) {
	setWorkerEnv(t, map[string]string{"WORKER_STREAM_BLOCK": "9223372036854775807ns", "WORKER_QUEUE_OPERATION_TIMEOUT": "1ns"})
	if _, err := config.LoadWorker(); err == nil {
		t.Fatal("LoadWorker() accepted overflowing block plus operation timeout")
	}
}

func TestLoadIgnoresMalformedWorkerAndScraperSettings(t *testing.T) {
	setWorkerEnv(t, map[string]string{
		"WORKER_POOL_SIZE": "invalid", "WORKER_CONSUMER_GROUP": "", "WORKER_CONSUMER_NAME": "", "SCRAPER_MAX_BODY_BYTES": "0", "SCRAPER_REQUEST_TIMEOUT": "broken",
	})
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("API Load() must ignore worker-only settings: %v", err)
	}
	if cfg.APIPort != "8080" || cfg.RedisStream != "inteldigest:jobs" {
		t.Errorf("API defaults changed: %+v", cfg)
	}
}

func setWorkerEnv(t *testing.T, values map[string]string) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	t.Setenv("REDIS_URL", "redis://localhost:6379/0")
	t.Setenv("REDIS_PUBLISH_TIMEOUT", "")

	optionalNames := []string{
		"REDIS_STREAM", "REDIS_DIAL_TIMEOUT", "WORKER_POOL_SIZE", "WORKER_CONSUMER_GROUP", "WORKER_CONSUMER_NAME",
		"WORKER_STREAM_BLOCK", "WORKER_QUEUE_OPERATION_TIMEOUT", "WORKER_DB_TIMEOUT", "WORKER_SHUTDOWN_TIMEOUT", "SCRAPER_CONNECT_TIMEOUT",
		"SCRAPER_TLS_TIMEOUT", "SCRAPER_HEADER_TIMEOUT", "SCRAPER_READ_TIMEOUT", "SCRAPER_REQUEST_TIMEOUT", "SCRAPER_MAX_BODY_BYTES", "SCRAPER_MAX_REDIRECTS",
	}
	type previousValue struct {
		value string
		set   bool
	}
	previous := make(map[string]previousValue, len(optionalNames))
	for _, name := range optionalNames {
		value, set := os.LookupEnv(name)
		previous[name] = previousValue{value: value, set: set}
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
	}
	t.Cleanup(func() {
		for name, old := range previous {
			var err error
			if old.set {
				err = os.Setenv(name, old.value)
			} else {
				err = os.Unsetenv(name)
			}
			if err != nil {
				t.Errorf("restore %s: %v", name, err)
			}
		}
	})
	for name, value := range values {
		if err := os.Setenv(name, value); err != nil {
			t.Fatalf("set %s: %v", name, err)
		}
	}
}
