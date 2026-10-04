package worker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"time"

	"github.com/google/uuid"
	"inteldigest/internal/models"
	"inteldigest/internal/queue"
	"inteldigest/internal/scraper"
)

// Repo is the minimal persistence boundary used by a single processor.
type Repo interface {
	GetJobByID(context.Context, uuid.UUID) (*models.Job, error)
	TryMarkProcessing(context.Context, uuid.UUID) (bool, error)
	RecordScrapeError(context.Context, uuid.UUID, string) (bool, error)
}

type Acker interface {
	Ack(context.Context, queue.Delivery) error
}
type ArticleScraper interface {
	ScrapeWithStats(context.Context, string) (scraper.Article, scraper.ScrapeStats, error)
}

type Options struct {
	DBTimeout             time.Duration
	QueueOperationTimeout time.Duration
	Logger                *slog.Logger
}

type Outcome string

const (
	OutcomeSkipped             Outcome = "skipped"
	OutcomeProcessingDuplicate Outcome = "processing_duplicate"
	OutcomeTerminalDuplicate   Outcome = "terminal_duplicate"
	OutcomeSucceeded           Outcome = "succeeded"
	OutcomeScrapeFailed        Outcome = "scrape_failed"
	OutcomeOperationalCanceled Outcome = "operational_canceled"
	OutcomeError               Outcome = "error"
)

type Processor struct {
	repo     Repo
	acker    Acker
	scraper  ArticleScraper
	receiver Receiver
	options  Options
	logger   *slog.Logger
}

// ProcessError exposes only a static safe message while retaining the cause for Is/As.
type ProcessError struct {
	code  string
	cause error
}

func (e *ProcessError) Error() string {
	if e == nil {
		return "worker processing error"
	}
	return "worker processing failed: " + e.code
}
func (e *ProcessError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func NewProcessor(repo Repo, acker Acker, articleScraper ArticleScraper, receiver Receiver, options Options) (*Processor, error) {
	if isNilDependency(repo) || isNilDependency(acker) || isNilDependency(articleScraper) || isNilDependency(receiver) {
		return nil, errors.New("worker processor dependencies are required")
	}
	if options.DBTimeout <= 0 || options.QueueOperationTimeout <= 0 {
		return nil, errors.New("worker operation timeouts must be positive")
	}
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Processor{repo: repo, acker: acker, scraper: articleScraper, receiver: receiver, options: options, logger: options.Logger}, nil
}

func (p *Processor) Process(admissionCtx, jobCtx context.Context, delivery queue.Delivery) (Outcome, error) {
	if p == nil || admissionCtx == nil || jobCtx == nil {
		return OutcomeError, &ProcessError{code: "invalid_context_or_processor"}
	}
	if err := jobCtx.Err(); err != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: err}
	}
	if err := admissionCtx.Err(); err != nil {
		return OutcomeSkipped, &ProcessError{code: "admission_closed", cause: err}
	}
	message, err := decodeDelivery(delivery)
	if err != nil {
		p.log("job_skipped", "reason", "invalid_payload", "stream_entry_id", delivery.ID)
		return OutcomeSkipped, &ProcessError{code: "invalid_payload"}
	}
	id := message.JobID
	if err := admissionCtx.Err(); err != nil {
		return OutcomeSkipped, &ProcessError{code: "admission_closed", cause: err}
	}
	if err := jobCtx.Err(); err != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: err}
	}

	preCtx, stopPre := boundedForParents(jobCtx, admissionCtx, p.options.DBTimeout)
	job, err := p.repo.GetJobByID(preCtx, id)
	stopPre()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return p.contextOutcome(admissionCtx, jobCtx, err)
		}
		p.logJob("job_skipped", id, "reason", "database_lookup_failed")
		return OutcomeError, &ProcessError{code: "database_lookup_failed", cause: err}
	}
	if admissionCtx.Err() != nil {
		return OutcomeSkipped, &ProcessError{code: "admission_closed", cause: admissionCtx.Err()}
	}
	if jobCtx.Err() != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: jobCtx.Err()}
	}
	if !validStoredJob(job, id, message.URL) {
		p.logJob("job_skipped", id, "reason", "job_mismatch_or_invalid")
		return OutcomeSkipped, &ProcessError{code: "job_mismatch_or_invalid"}
	}
	if job.Status == models.StatusCompleted || job.Status == models.StatusFailed {
		return p.ackTerminal(admissionCtx, jobCtx, delivery, id)
	}
	if job.Status == models.StatusProcessing {
		p.logJob("job_skipped", id, "reason", "already_processing")
		return OutcomeProcessingDuplicate, nil
	}
	if job.Status != models.StatusPending {
		p.logJob("job_skipped", id, "reason", "unknown_status")
		return OutcomeSkipped, &ProcessError{code: "unknown_status"}
	}
	if err := admissionCtx.Err(); err != nil {
		return OutcomeSkipped, &ProcessError{code: "admission_closed", cause: err}
	}
	if err := jobCtx.Err(); err != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: err}
	}

	claimCtx, stopClaim := boundedForParents(jobCtx, admissionCtx, p.options.DBTimeout)
	claimed, claimErr := p.repo.TryMarkProcessing(claimCtx, id)
	stopClaim()
	if claimErr != nil {
		if errors.Is(claimErr, context.Canceled) || errors.Is(claimErr, context.DeadlineExceeded) {
			return p.contextOutcome(admissionCtx, jobCtx, claimErr)
		}
		p.logJob("processing_claim_failed", id, "operation", "claim")
		return OutcomeError, &ProcessError{code: "processing_claim_failed", cause: claimErr}
	}
	if !claimed {
		freshCtx, stopFresh := boundedForParents(jobCtx, admissionCtx, p.options.DBTimeout)
		fresh, freshErr := p.repo.GetJobByID(freshCtx, id)
		stopFresh()
		if freshErr != nil {
			if errors.Is(freshErr, context.Canceled) || errors.Is(freshErr, context.DeadlineExceeded) {
				return p.contextOutcome(admissionCtx, jobCtx, freshErr)
			}
			p.logJob("claim_lost_requery_failed", id, "operation", "requery")
			return OutcomeError, &ProcessError{code: "claim_lost_requery_failed", cause: freshErr}
		}
		if admissionCtx.Err() != nil {
			return OutcomeSkipped, &ProcessError{code: "admission_closed", cause: admissionCtx.Err()}
		}
		if jobCtx.Err() != nil {
			return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: jobCtx.Err()}
		}
		if !validStoredJob(fresh, id, message.URL) {
			return OutcomeSkipped, &ProcessError{code: "claim_lost_requery_invalid"}
		}
		if fresh.Status == models.StatusCompleted || fresh.Status == models.StatusFailed {
			return p.ackTerminal(admissionCtx, jobCtx, delivery, id)
		}
		if fresh.Status == models.StatusProcessing {
			p.logJob("job_skipped", id, "reason", "claim_lost_processing")
			return OutcomeProcessingDuplicate, nil
		}
		p.logJob("job_skipped", id, "reason", "claim_lost_unresolved")
		return OutcomeSkipped, &ProcessError{code: "claim_lost_unresolved"}
	}
	p.logJob("processing_started", id)
	if err := jobCtx.Err(); err != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: err}
	}
	started := time.Now()
	article, stats, scrapeErr := p.scraper.ScrapeWithStats(jobCtx, job.URL)
	duration := time.Since(started)
	if scrapeErr == nil {
		if err := jobCtx.Err(); err != nil {
			return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: err}
		}
		result := Result{JobID: id, Article: &article}
		if err := validateResult(result); err != nil {
			return OutcomeError, &ProcessError{code: "invalid_article_result", cause: err}
		}
		if err := p.receiver.Receive(jobCtx, result); err != nil {
			p.logJob("result_delivery_failed", id, "operation", "receiver")
			return OutcomeError, &ProcessError{code: "result_delivery_failed", cause: err}
		}
		p.logJob("scraping_completed", id, "duration", duration, "bytes_read", stats.BytesRead)
		return OutcomeSucceeded, nil
	}
	if jobCtx.Err() != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: jobCtx.Err()}
	}
	se := safeScrapeError(scrapeErr)
	classification := classify(se, scrapeErr)
	reason := diagnosticReason(se)
	dbCtx, cancel := context.WithTimeout(jobCtx, p.options.DBTimeout)
	recorded, recordErr := p.repo.RecordScrapeError(dbCtx, id, reason)
	cancel()
	if recordErr != nil || !recorded {
		p.logJob("scrape_error_record_failed", id, "classification", classification, "kind", safeKind(se.Kind), "stage", safeStage(se.Stage), "status", safeStatus(se.HTTPStatus))
		if recordErr != nil {
			p.logJob("scrape_diagnostic_persistence_problem", id, "operation", "record_scrape_error")
		}
	}
	if jobCtx.Err() != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: jobCtx.Err()}
	}
	result := Result{JobID: id, Error: se}
	if err := validateResult(result); err != nil {
		return OutcomeError, &ProcessError{code: "invalid_scrape_result", cause: err}
	}
	if err := p.receiver.Receive(jobCtx, result); err != nil {
		p.logJob("result_delivery_failed", id, "operation", "receiver")
		return OutcomeError, &ProcessError{code: "result_delivery_failed", cause: err}
	}
	p.logJob("scraping_failed", id, "classification", classification, "kind", safeKind(se.Kind), "stage", safeStage(se.Stage), "status", safeStatus(se.HTTPStatus))
	return OutcomeScrapeFailed, nil
}

func decodeDelivery(d queue.Delivery) (queue.Message, error) {
	var m queue.Message
	if d.Fields == nil {
		return m, errors.New("missing payload")
	}
	raw, ok := d.Fields["data"].(string)
	if !ok || raw == "" {
		return m, errors.New("invalid payload field")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &object); err != nil || object == nil {
		return m, errors.New("invalid payload object")
	}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return m, errors.New("invalid payload")
	}
	if err := m.Validate(); err != nil {
		return m, errors.New("invalid payload")
	}
	return m, nil
}
func validStoredJob(job *models.Job, id uuid.UUID, url string) bool {
	return job != nil && job.ID == id && job.URL == url
}
func boundedForParents(primary, other context.Context, d time.Duration) (context.Context, func()) {
	ctx, cancel := context.WithTimeout(primary, d)
	stop := context.AfterFunc(other, cancel)
	return ctx, func() { stop(); cancel() }
}
func (p *Processor) ackTerminal(admission, job context.Context, d queue.Delivery, id uuid.UUID) (Outcome, error) {
	if job.Err() != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: job.Err()}
	}
	if admission.Err() != nil {
		return OutcomeSkipped, &ProcessError{code: "admission_closed", cause: admission.Err()}
	}
	ctx, cancel := boundedForParents(job, admission, p.options.QueueOperationTimeout)
	err := p.acker.Ack(ctx, d)
	cancel()
	if err != nil {
		p.logJob("job_ack_failed", id, "stream_entry_id", d.ID)
		return OutcomeError, &ProcessError{code: "ack_failed", cause: err}
	}
	p.logJob("job_acked", id, "stream_entry_id", d.ID)
	return OutcomeTerminalDuplicate, nil
}
func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (p *Processor) contextOutcome(admission, job context.Context, cause error) (Outcome, error) {
	if job.Err() != nil {
		return OutcomeOperationalCanceled, &ProcessError{code: "job_context_canceled", cause: job.Err()}
	}
	if admission.Err() != nil {
		return OutcomeSkipped, &ProcessError{code: "admission_closed", cause: admission.Err()}
	}
	return OutcomeError, &ProcessError{code: "operation_timeout", cause: cause}
}
func (p *Processor) log(event string, args ...any) { p.logger.Info(event, args...) }
func (p *Processor) logJob(event string, id uuid.UUID, args ...any) {
	base := []any{"job_id", id.String()}
	p.log(event, append(base, args...)...)
}
func safeScrapeError(err error) *scraper.ScrapeError {
	var typed *scraper.ScrapeError
	if errors.As(err, &typed) && typed != nil {
		return &scraper.ScrapeError{Kind: typed.Kind, Stage: typed.Stage, HTTPStatus: typed.HTTPStatus, Message: typed.Error(), Cause: typed}
	}
	kind := scraper.ScrapeErrorNetwork
	stage := scraper.ScrapeStageFetch
	if errors.Is(err, context.DeadlineExceeded) {
		kind = scraper.ScrapeErrorTimeout
	}
	return &scraper.ScrapeError{Kind: kind, Stage: stage, Message: "scrape failed", Cause: err}
}
func safeKind(k scraper.ScrapeErrorKind) string {
	switch k {
	case scraper.ScrapeErrorSecurityRejected, scraper.ScrapeErrorTimeout, scraper.ScrapeErrorNetwork, scraper.ScrapeErrorCanceled, scraper.ScrapeErrorBodyTooLarge, scraper.ScrapeErrorHTTPStatus, scraper.ScrapeErrorUnsupported, scraper.ScrapeErrorExtraction:
		return string(k)
	}
	return "unknown"
}
func safeStage(s scraper.ScrapeStage) string {
	switch s {
	case scraper.ScrapeStageValidation, scraper.ScrapeStageFetch, scraper.ScrapeStageExtract:
		return string(s)
	}
	return "unknown"
}
func safeStatus(n int) int {
	if n >= 100 && n <= 599 {
		return n
	}
	return 0
}
func diagnosticReason(e *scraper.ScrapeError) string {
	if e.HTTPStatus >= 100 && e.HTTPStatus <= 599 {
		return fmt.Sprintf("scraping/%s: HTTP %d", safeKind(e.Kind), e.HTTPStatus)
	}
	return "scraping/" + safeKind(e.Kind) + " during " + safeStage(e.Stage)
}
func classify(e *scraper.ScrapeError, cause error) string {
	var invalid x509.CertificateInvalidError
	var unknown x509.UnknownAuthorityError
	var verify *tls.CertificateVerificationError
	if errors.As(cause, &invalid) || errors.As(cause, &unknown) || errors.As(cause, &verify) {
		return "permanent"
	}
	switch e.Kind {
	case scraper.ScrapeErrorSecurityRejected, scraper.ScrapeErrorBodyTooLarge, scraper.ScrapeErrorUnsupported, scraper.ScrapeErrorExtraction:
		return "permanent"
	case scraper.ScrapeErrorTimeout, scraper.ScrapeErrorNetwork:
		return "transient"
	case scraper.ScrapeErrorHTTPStatus:
		if e.HTTPStatus == 408 || e.HTTPStatus == 429 || e.HTTPStatus >= 500 {
			return "transient"
		}
		if e.HTTPStatus >= 400 && e.HTTPStatus < 500 {
			return "permanent"
		}
	}
	var netErr net.Error
	if errors.As(cause, &netErr) {
		return "transient"
	}
	return "unknown"
}
