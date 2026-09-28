//go:build integration

package db_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"inteldigest/internal/db"
	"inteldigest/internal/models"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

var testPool *pgxpool.Pool

const testDBName = "inteldigest_test"

func TestMain(m *testing.M) {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required for integration tests")
	}

	// Connect to default database to create the test database.
	adminPool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connecting to admin db: %v", err)
	}

	// Drop if leftover, then create fresh.
	adminPool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDBName))
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", testDBName)); err != nil {
		log.Fatalf("creating test db: %v", err)
	}
	adminPool.Close()

	// Build DSN for the test database.
	testDSN := replaceDBName(dsn, testDBName)

	testPool, err = pgxpool.New(ctx, testDSN)
	if err != nil {
		log.Fatalf("connecting to test db: %v", err)
	}

	// Apply migrations.
	if err := applyMigrations(ctx, testPool); err != nil {
		log.Fatalf("applying migrations: %v", err)
	}

	code := m.Run()

	// Cleanup.
	testPool.Close()
	cleanupPool, _ := pgxpool.New(ctx, dsn)
	if cleanupPool != nil {
		cleanupPool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDBName))
		cleanupPool.Close()
	}

	os.Exit(code)
}

// newTestRepo starts a transaction and returns a Repository bound to it.
// The transaction is rolled back in t.Cleanup, isolating each test.
func newTestRepo(t *testing.T) *db.Repository {
	t.Helper()
	ctx := context.Background()
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	t.Cleanup(func() {
		tx.Rollback(ctx)
	})
	return db.NewRepository(tx)
}

func TestCreateJob(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	job, err := repo.CreateJob(ctx, "https://example.com/article")
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	if job.ID.String() == "" {
		t.Fatal("expected non-empty job ID")
	}
	if job.URL != "https://example.com/article" {
		t.Errorf("URL = %q", job.URL)
	}
	if job.Status != models.StatusPending {
		t.Errorf("Status = %q, want pending", job.Status)
	}
	if job.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
}

func TestGetJobByID(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	// Create a job first.
	created, err := repo.CreateJob(ctx, "https://example.com/test")
	if err != nil {
		t.Fatalf("CreateJob: %v", err)
	}

	// Retrieve it.
	got, err := repo.GetJobByID(ctx, created.ID)
	if err != nil {
		t.Fatalf("GetJobByID: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("ID = %v, want %v", got.ID, created.ID)
	}
	if got.URL != "https://example.com/test" {
		t.Errorf("URL = %q", got.URL)
	}
	if got.Status != models.StatusPending {
		t.Errorf("Status = %q", got.Status)
	}
}

func TestGetJobByIDNotFound(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	_, err := repo.GetJobByID(ctx, uuid.New())
	if err == nil {
		t.Fatal("expected error for non-existent job")
	}
	if !errors.Is(err, db.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

// --- helpers ---

func replaceDBName(dsn, newDB string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		log.Fatalf("parsing DSN: %v", err)
	}
	u.Path = "/" + newDB
	return u.String()
}

func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	files, err := filepath.Glob("../../migrations/*.up.sql")
	if err != nil {
		return fmt.Errorf("glob migrations: %w", err)
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read %s: %w", f, err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("exec %s: %w", f, err)
		}
	}
	return nil
}

// containsString is a test helper for substring matching.
func containsString(s, sub string) bool {
	return strings.Contains(s, sub)
}

// pgErrCode extracts the Postgres error code if available.
func pgErrCode(err error) string {
	var pgErr interface{ SQLState() string }
	if ok := errors.As(err, &pgErr); ok {
		return pgErr.SQLState()
	}
	return ""
}

// Ensure errors import is used.
var _ = pgErrCode
