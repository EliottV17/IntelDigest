package scraper

import (
	"fmt"
	"time"
)

// Article contains the extracted text and the URLs needed by downstream stages.
type Article struct {
	Text         string
	RequestedURL string
	FinalURL     string
	Title        string
	Language     string
	FetchedAt    time.Time
}

// ScrapeStats contains per-call fetch measurements. BytesRead is the number of
// decompressed final-response body bytes obtained after a successful bounded
// body read, before charset conversion or extraction. It excludes redirect
// bodies and is not a transport-byte or extracted-text length. It remains zero
// until the complete body is available; failures before then do not report
// partial reads. If decoding or extraction later fails, the completed body count
// is retained.
type ScrapeStats struct {
	BytesRead int64
}

type ScrapeErrorKind string

const (
	ScrapeErrorSecurityRejected ScrapeErrorKind = "security_rejected"
	ScrapeErrorTimeout          ScrapeErrorKind = "timeout"
	ScrapeErrorNetwork          ScrapeErrorKind = "network"
	ScrapeErrorCanceled         ScrapeErrorKind = "canceled"
	ScrapeErrorBodyTooLarge     ScrapeErrorKind = "body_too_large"
	ScrapeErrorHTTPStatus       ScrapeErrorKind = "http_status"
	ScrapeErrorUnsupported      ScrapeErrorKind = "unsupported_content"
	ScrapeErrorExtraction       ScrapeErrorKind = "extraction"
)

type ScrapeStage string

const (
	ScrapeStageValidation ScrapeStage = "validation"
	ScrapeStageFetch      ScrapeStage = "fetch"
	ScrapeStageExtract    ScrapeStage = "extract"
)

// ScrapeError keeps its cause available to errors.Is/As while Error deliberately
// emits only controlled classification fields, never Message or Cause.
type ScrapeError struct {
	Kind       ScrapeErrorKind
	Stage      ScrapeStage
	HTTPStatus int
	Message    string
	Cause      error
}

func (e *ScrapeError) Error() string {
	if e == nil {
		return "scrape error"
	}
	kind := "unknown"
	switch e.Kind {
	case ScrapeErrorSecurityRejected, ScrapeErrorTimeout, ScrapeErrorNetwork, ScrapeErrorCanceled,
		ScrapeErrorBodyTooLarge, ScrapeErrorHTTPStatus, ScrapeErrorUnsupported, ScrapeErrorExtraction:
		kind = string(e.Kind)
	}
	stage := "unknown"
	switch e.Stage {
	case ScrapeStageValidation, ScrapeStageFetch, ScrapeStageExtract:
		stage = string(e.Stage)
	}
	if e.HTTPStatus >= 100 && e.HTTPStatus <= 599 {
		return fmt.Sprintf("scrape %s during %s (HTTP %d)", kind, stage, e.HTTPStatus)
	}
	return fmt.Sprintf("scrape %s during %s", kind, stage)
}

func (e *ScrapeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}
