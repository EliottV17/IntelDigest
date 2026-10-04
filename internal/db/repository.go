// Package db provides PostgreSQL access for IntelDigest jobs.
package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"inteldigest/internal/models"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// ErrNotFound is returned when a job does not exist.
var ErrNotFound = errors.New("job not found")

const maxScrapeErrorLength = 1024

// DBTX is satisfied by both *pgxpool.Pool and pgx.Tx,
// allowing the repository to work with either.
type DBTX interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Repository handles job persistence in PostgreSQL.
type Repository struct {
	db DBTX
}

// NewRepository creates a Repository backed by the given DBTX.
func NewRepository(db DBTX) *Repository {
	return &Repository{db: db}
}

// CreateJob inserts a new job in pending status and returns it.
func (r *Repository) CreateJob(ctx context.Context, url string) (*models.Job, error) {
	id := uuid.New()
	now := time.Now().UTC()

	_, err := r.db.Exec(ctx,
		`INSERT INTO jobs (id, url, status, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		id, url, models.StatusPending, now, now,
	)
	if err != nil {
		return nil, err
	}

	return &models.Job{
		ID:        id,
		URL:       url,
		Status:    models.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

// TryMarkProcessing atomically claims a pending job for processing.
func (r *Repository) TryMarkProcessing(ctx context.Context, id uuid.UUID) (bool, error) {
	tag, err := r.db.Exec(ctx,
		`UPDATE jobs
		 SET status = 'processing', error = NULL, updated_at = NOW()
		 WHERE id = $1 AND status = 'pending'`,
		id,
	)
	if err != nil {
		return false, fmt.Errorf("mark job processing: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// RecordScrapeError stores a bounded diagnostic without changing job status.
func (r *Repository) RecordScrapeError(ctx context.Context, id uuid.UUID, safeReason string) (bool, error) {
	if runes := []rune(safeReason); len(runes) > maxScrapeErrorLength {
		safeReason = string(runes[:maxScrapeErrorLength])
	}

	tag, err := r.db.Exec(ctx,
		`UPDATE jobs
		 SET error = $2, updated_at = NOW()
		 WHERE id = $1 AND status = 'processing'`,
		id, safeReason,
	)
	if err != nil {
		return false, fmt.Errorf("record scrape error: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// GetJobByID retrieves a job by its UUID. Returns ErrNotFound if it does not exist.
func (r *Repository) GetJobByID(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	var j models.Job
	var result []byte
	var jobErr *string

	err := r.db.QueryRow(ctx,
		`SELECT id, url, status, result, error, created_at, updated_at
		 FROM jobs WHERE id = $1`, id,
	).Scan(&j.ID, &j.URL, &j.Status, &result, &jobErr, &j.CreatedAt, &j.UpdatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if result != nil {
		raw := json.RawMessage(result)
		j.Result = &raw
	}
	j.Error = jobErr

	return &j, nil
}
