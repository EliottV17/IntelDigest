package scraper

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	readability "codeberg.org/readeck/go-readability/v2"
	"golang.org/x/net/html/charset"
)

const (
	defaultScraperConnectTimeout = 5 * time.Second
	defaultScraperTLSTimeout     = 5 * time.Second
	defaultScraperHeaderTimeout  = 10 * time.Second
	defaultScraperReadTimeout    = 10 * time.Second
	defaultScraperRequestTimeout = 30 * time.Second
	defaultScraperMaxBodyBytes   = int64(5 << 20)
	defaultScraperMaxRedirects   = 5
	maxScraperRedirects          = 10
	contentSniffBytes            = 512
)

// ScraperOptions contains the explicit network and response limits for a scraper.
type ScraperOptions struct {
	ConnectTimeout time.Duration
	TLSTimeout     time.Duration
	HeaderTimeout  time.Duration
	ReadTimeout    time.Duration
	RequestTimeout time.Duration
	MaxBodyBytes   int64
	MaxRedirects   int
}

// DefaultOptions returns the approved conservative scraper budgets.
func DefaultOptions() ScraperOptions {
	return ScraperOptions{
		ConnectTimeout: defaultScraperConnectTimeout,
		TLSTimeout:     defaultScraperTLSTimeout,
		HeaderTimeout:  defaultScraperHeaderTimeout,
		ReadTimeout:    defaultScraperReadTimeout,
		RequestTimeout: defaultScraperRequestTimeout,
		MaxBodyBytes:   defaultScraperMaxBodyBytes,
		MaxRedirects:   defaultScraperMaxRedirects,
	}
}

// Scraper fetches one public HTML article and extracts its useful text.
type Scraper struct {
	client    *http.Client
	options   ScraperOptions
	extractor articleExtractor
	clock     func() time.Time
}

type extractedArticle struct {
	text     string
	title    string
	language string
}

type articleExtractor func(io.Reader, *url.URL) (extractedArticle, error)

// NewScraper constructs a scraper with no caller-controlled transport or
// private-destination override. Zero-valued limits are invalid; use DefaultOptions
// and change only the desired fields when defaults are wanted.
func NewScraper(options ScraperOptions) (*Scraper, error) {
	return newScraper(options, nil, nil, nil, nil, nil)
}

func newScraper(options ScraperOptions, resolver addressResolver, dialer contextDialer, extractor articleExtractor, clock func() time.Time, roots *x509.CertPool) (*Scraper, error) {
	if err := validateScraperOptions(options); err != nil {
		return nil, err
	}
	if extractor == nil {
		extractor = extractReadability
	}
	if clock == nil {
		clock = time.Now
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           newSafeDialer(options.ConnectTimeout, resolver, dialer),
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots},
		TLSHandshakeTimeout:   options.TLSTimeout,
		ResponseHeaderTimeout: options.HeaderTimeout,
		DisableKeepAlives:     true,
		ForceAttemptHTTP2:     false,
		TLSNextProto:          make(map[string]func(string, *tls.Conn) http.RoundTripper),
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &Scraper{client: client, options: options, extractor: extractor, clock: clock}, nil
}

func validateScraperOptions(options ScraperOptions) error {
	if options.ConnectTimeout <= 0 || options.TLSTimeout <= 0 || options.HeaderTimeout <= 0 || options.ReadTimeout <= 0 || options.RequestTimeout <= 0 {
		return errors.New("scraper: all time limits must be positive")
	}
	if options.MaxBodyBytes <= 0 || options.MaxBodyBytes >= int64(^uint(0)>>1) {
		return errors.New("scraper: body limit must be positive and allow a safe max+1 read")
	}
	if options.MaxRedirects < 0 || options.MaxRedirects > maxScraperRedirects {
		return fmt.Errorf("scraper: redirect limit must be between 0 and %d", maxScraperRedirects)
	}
	return nil
}

// Scrape validates, fetches and extracts an article. It makes no external
// requests except to the requested public URL and policy-approved redirects.
func (s *Scraper) Scrape(parent context.Context, rawURL string) (Article, error) {
	article, _, err := s.ScrapeWithStats(parent, rawURL)
	return article, err
}

// ScrapeWithStats performs the same scrape as Scrape and returns measurements
// local to this invocation.
func (s *Scraper) ScrapeWithStats(parent context.Context, rawURL string) (Article, ScrapeStats, error) {
	var stats ScrapeStats
	article, err := s.scrape(parent, rawURL, &stats)
	return article, stats, err
}

func (s *Scraper) scrape(parent context.Context, rawURL string, stats *ScrapeStats) (Article, error) {
	if parent == nil {
		return Article{}, &ScrapeError{Kind: ScrapeErrorCanceled, Stage: ScrapeStageFetch, Message: "scrape context is unavailable"}
	}
	if err := parent.Err(); err != nil {
		return Article{}, &ScrapeError{Kind: contextErrorKind(err), Stage: ScrapeStageFetch, Message: "scrape was canceled", Cause: err}
	}
	current, err := validateURL(rawURL)
	if err != nil {
		return Article{}, err
	}
	ctx, cancel := context.WithTimeout(parent, s.options.RequestTimeout)
	defer cancel()

	for redirects := 0; ; redirects++ {
		if err := ctx.Err(); err != nil {
			return Article{}, scrapeContextError(err)
		}
		hopCtx, cancelHop := context.WithCancel(ctx)
		request, err := http.NewRequestWithContext(hopCtx, http.MethodGet, current.String(), nil)
		if err != nil {
			cancelHop()
			return Article{}, &ScrapeError{Kind: ScrapeErrorSecurityRejected, Stage: ScrapeStageValidation, Message: "request URL rejected", Cause: ErrSecurityRejected}
		}
		request.Header.Set("Accept", "text/html, application/xhtml+xml")
		response, requestErr := s.client.Do(request)
		if requestErr != nil {
			failure := classifyScrapeFailure(hopCtx, requestErr, nil)
			cancelHop()
			closeResponse(response)
			return Article{}, failure
		}
		finishResponse := func() {
			cancelHop()
			closeResponse(response)
		}
		if isRedirectStatus(response.StatusCode) {
			location := response.Header.Get("Location")
			finishResponse()
			if location == "" {
				return Article{}, httpStatusError(response.StatusCode)
			}
			if redirects >= s.options.MaxRedirects {
				return Article{}, securityError(ScrapeStageFetch, "redirect limit exceeded")
			}
			reference, parseErr := url.Parse(location)
			if parseErr != nil {
				return Article{}, securityError(ScrapeStageValidation, "redirect URL rejected")
			}
			next := current.ResolveReference(reference)
			validated, validationErr := validateURL(next.String())
			if validationErr != nil {
				return Article{}, validationErr
			}
			if current.Scheme == "https" && validated.Scheme == "http" {
				return Article{}, securityError(ScrapeStageValidation, "HTTPS downgrade rejected")
			}
			current = validated
			continue
		}
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			finishResponse()
			return Article{}, httpStatusError(response.StatusCode)
		}

		body, readErr := s.readBody(hopCtx, cancelHop, response)
		finishResponse()
		if readErr != nil {
			return Article{}, classifyScrapeFailure(hopCtx, readErr, readErr)
		}
		stats.BytesRead = int64(len(body))
		if err := ctx.Err(); err != nil {
			return Article{}, scrapeContextError(err)
		}
		decoded, decodeErr := charset.NewReader(bytes.NewReader(body), response.Header.Get("Content-Type"))
		if decodeErr != nil {
			return Article{}, &ScrapeError{Kind: ScrapeErrorUnsupported, Stage: ScrapeStageFetch, Message: "HTML charset is unsupported", Cause: decodeErr}
		}
		if err := ctx.Err(); err != nil {
			return Article{}, scrapeContextError(err)
		}
		parsed, extractionErr := s.extractor(decoded, current)
		if extractionErr != nil {
			return Article{}, &ScrapeError{Kind: ScrapeErrorExtraction, Stage: ScrapeStageExtract, Message: "article extraction failed", Cause: extractionErr}
		}
		text := strings.TrimSpace(parsed.text)
		if text == "" {
			return Article{}, &ScrapeError{Kind: ScrapeErrorExtraction, Stage: ScrapeStageExtract, Message: "page contains no useful article text"}
		}
		if err := ctx.Err(); err != nil {
			return Article{}, scrapeContextError(err)
		}
		return Article{
			Text: text, RequestedURL: rawURL, FinalURL: current.String(),
			Title: parsed.title, Language: parsed.language, FetchedAt: s.clock().UTC(),
		}, nil
	}
}

func (s *Scraper) readBody(ctx context.Context, cancel context.CancelFunc, response *http.Response) ([]byte, error) {
	boundedBody := &limitedResponseBody{
		reader: response.Body,
		closer: response.Body,
		left:   s.options.MaxBodyBytes + 1,
	}
	response.Body = boundedBody
	var readExpired atomic.Bool
	timerDone := make(chan struct{})
	timer := time.AfterFunc(s.options.ReadTimeout, func() {
		defer close(timerDone)
		readExpired.Store(true)
		cancel()
	})
	timerStopped := false
	stopTimer := func() bool {
		if timerStopped {
			return readExpired.Load()
		}
		timerStopped = true
		if !timer.Stop() {
			<-timerDone
		}
		return readExpired.Load()
	}
	defer stopTimer()
	if encoding := strings.TrimSpace(response.Header.Get("Content-Encoding")); encoding != "" {
		return nil, &ScrapeError{Kind: ScrapeErrorUnsupported, Stage: ScrapeStageFetch, Message: "response content encoding is unsupported"}
	}
	if response.ContentLength > s.options.MaxBodyBytes {
		return nil, &ScrapeError{Kind: ScrapeErrorBodyTooLarge, Stage: ScrapeStageFetch, Message: "response exceeds configured body limit"}
	}
	mediaType, parameters, mimeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.Header.Get("Content-Type") == "" {
		limitedBody := response.Body
		buffered := bufio.NewReaderSize(limitedBody, contentSniffBytes)
		prefix, peekErr := buffered.Peek(contentSniffBytes)
		if boundedBody.readBytes > s.options.MaxBodyBytes {
			return nil, &ScrapeError{Kind: ScrapeErrorBodyTooLarge, Stage: ScrapeStageFetch, Message: "response exceeds configured body limit"}
		}
		if peekErr != nil && !errors.Is(peekErr, io.EOF) && !errors.Is(peekErr, bufio.ErrBufferFull) {
			return nil, readBodyError(ctx, stopTimer(), peekErr)
		}
		if !looksLikeHTML(prefix) {
			return nil, &ScrapeError{Kind: ScrapeErrorUnsupported, Stage: ScrapeStageFetch, Message: "response is not recognizable HTML"}
		}
		response.Body = &bufferedResponseBody{reader: buffered, closer: limitedBody}
	} else if mimeErr != nil || !isHTMLMediaType(mediaType) {
		return nil, &ScrapeError{Kind: ScrapeErrorUnsupported, Stage: ScrapeStageFetch, Message: "response MIME type is unsupported", Cause: mimeErr}
	}
	if label := parameters["charset"]; label != "" {
		if encoding, _ := charset.Lookup(label); encoding == nil {
			return nil, &ScrapeError{Kind: ScrapeErrorUnsupported, Stage: ScrapeStageFetch, Message: "HTML charset is unsupported"}
		}
	}
	limited := io.LimitReader(response.Body, s.options.MaxBodyBytes+1)
	body, err := io.ReadAll(limited)
	if int64(len(body)) > s.options.MaxBodyBytes {
		return nil, &ScrapeError{Kind: ScrapeErrorBodyTooLarge, Stage: ScrapeStageFetch, Message: "response exceeds configured body limit"}
	}
	if err != nil {
		return nil, readBodyError(ctx, stopTimer(), err)
	}
	if ctx.Err() != nil {
		return nil, readBodyError(ctx, stopTimer(), ctx.Err())
	}
	if stopTimer() {
		return nil, readBodyError(ctx, true, context.Canceled)
	}
	return body, nil
}

// limitedResponseBody bounds bytes exposed by the transport's decompressed body.
type limitedResponseBody struct {
	reader    io.Reader
	closer    io.Closer
	left      int64
	readBytes int64
}

func (body *limitedResponseBody) Read(data []byte) (int, error) {
	if body.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(data)) > body.left {
		data = data[:body.left]
	}
	n, err := body.reader.Read(data)
	body.left -= int64(n)
	body.readBytes += int64(n)
	return n, err
}

func (body *limitedResponseBody) Close() error { return body.closer.Close() }

type bufferedResponseBody struct {
	reader *bufio.Reader
	closer io.Closer
}

func (b *bufferedResponseBody) Read(data []byte) (int, error) { return b.reader.Read(data) }
func (b *bufferedResponseBody) Close() error                  { return b.closer.Close() }

func readBodyError(ctx context.Context, readExpired bool, cause error) error {
	if readExpired || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &ScrapeError{Kind: ScrapeErrorTimeout, Stage: ScrapeStageFetch, Message: "response read budget expired", Cause: cause}
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(cause, context.Canceled) {
		return &ScrapeError{Kind: ScrapeErrorCanceled, Stage: ScrapeStageFetch, Message: "response read was canceled", Cause: cause}
	}
	return &ScrapeError{Kind: ScrapeErrorNetwork, Stage: ScrapeStageFetch, Message: "response body could not be read", Cause: cause}
}

func extractReadability(input io.Reader, pageURL *url.URL) (extractedArticle, error) {
	article, err := readability.FromReader(input, pageURL)
	if err != nil {
		return extractedArticle{}, err
	}
	var text strings.Builder
	if err := article.RenderText(&text); err != nil {
		return extractedArticle{}, err
	}
	return extractedArticle{text: text.String(), title: article.Title(), language: article.Language()}, nil
}

func isHTMLMediaType(mediaType string) bool {
	return mediaType == "text/html" || mediaType == "application/xhtml+xml"
}

func looksLikeHTML(prefix []byte) bool {
	value := strings.TrimSpace(strings.TrimPrefix(string(prefix), "\xef\xbb\xbf"))
	value = strings.ToLower(value)
	for _, marker := range []string{"<!doctype html", "<html", "<head", "<body", "<article"} {
		if strings.HasPrefix(value, marker) {
			return true
		}
	}
	return false
}

func isRedirectStatus(status int) bool {
	switch status {
	case http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect:
		return true
	default:
		return false
	}
}

func closeResponse(response *http.Response) {
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
}

func httpStatusError(status int) error {
	return &ScrapeError{Kind: ScrapeErrorHTTPStatus, Stage: ScrapeStageFetch, HTTPStatus: status, Message: "HTTP response status was not accepted"}
}

func securityError(stage ScrapeStage, message string) error {
	return &ScrapeError{Kind: ScrapeErrorSecurityRejected, Stage: stage, Message: message, Cause: ErrSecurityRejected}
}

func scrapeContextError(err error) error {
	kind := contextErrorKind(err)
	message := "scrape request timed out"
	if kind == ScrapeErrorCanceled {
		message = "scrape request was canceled"
	}
	return &ScrapeError{Kind: kind, Stage: ScrapeStageFetch, Message: message, Cause: err}
}

func classifyScrapeFailure(ctx context.Context, err error, cause error) error {
	var existing *ScrapeError
	if errors.As(err, &existing) {
		return existing
	}
	if cause == nil {
		cause = err
	}
	if ctx.Err() != nil {
		return scrapeContextError(ctx.Err())
	}
	if errors.Is(err, context.Canceled) {
		return &ScrapeError{Kind: ScrapeErrorCanceled, Stage: ScrapeStageFetch, Message: "scrape request was canceled", Cause: cause}
	}
	var networkError net.Error
	if errors.As(err, &networkError) && networkError.Timeout() {
		return &ScrapeError{Kind: ScrapeErrorTimeout, Stage: ScrapeStageFetch, Message: "network stage timed out", Cause: cause}
	}
	return &ScrapeError{Kind: ScrapeErrorNetwork, Stage: ScrapeStageFetch, Message: "network request failed", Cause: cause}
}
