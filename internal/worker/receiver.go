package worker

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"inteldigest/internal/scraper"
)

// Result is the synchronous in-memory handoff for one claimed job.
type Result struct {
	JobID   uuid.UUID
	Article *scraper.Article
	Error   *scraper.ScrapeError
}

// Receiver must consume results synchronously and honor cancellation.
type Receiver interface {
	Receive(context.Context, Result) error
}

func validJobID(id uuid.UUID) bool { return id != uuid.Nil }

func validateResult(result Result) error {
	if !validJobID(result.JobID) {
		return errors.New("invalid worker result job ID")
	}
	if (result.Article == nil) == (result.Error == nil) {
		return errors.New("worker result must contain exactly one article or scrape error")
	}
	return nil
}
