package scraper

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"net/netip"
)

const articleURL = "http://news.example/article"

func TestScraperOptionsAreExplicitAndBounded(t *testing.T) {
	defaults := DefaultOptions()
	if defaults.ConnectTimeout != 5*time.Second || defaults.TLSTimeout != 5*time.Second || defaults.HeaderTimeout != 10*time.Second || defaults.ReadTimeout != 10*time.Second || defaults.RequestTimeout != 30*time.Second || defaults.MaxBodyBytes != 5<<20 || defaults.MaxRedirects != 5 {
		t.Fatalf("unexpected defaults: %+v", defaults)
	}
	valid := defaults
	valid.MaxRedirects = 0
	if _, err := NewScraper(valid); err != nil {
		t.Fatalf("zero redirects must be a valid explicit setting: %v", err)
	}
	invalid := []struct {
		name string
		edit func(*ScraperOptions)
	}{
		{"zero connect timeout", func(o *ScraperOptions) { o.ConnectTimeout = 0 }},
		{"negative TLS timeout", func(o *ScraperOptions) { o.TLSTimeout = -time.Second }},
		{"zero header timeout", func(o *ScraperOptions) { o.HeaderTimeout = 0 }},
		{"zero body timeout", func(o *ScraperOptions) { o.ReadTimeout = 0 }},
		{"zero request timeout", func(o *ScraperOptions) { o.RequestTimeout = 0 }},
		{"zero body limit", func(o *ScraperOptions) { o.MaxBodyBytes = 0 }},
		{"body limit max plus one overflow", func(o *ScraperOptions) { o.MaxBodyBytes = int64(^uint64(0) >> 1) }},
		{"negative redirect count", func(o *ScraperOptions) { o.MaxRedirects = -1 }},
		{"too many redirects", func(o *ScraperOptions) { o.MaxRedirects = 11 }},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			options := defaults
			test.edit(&options)
			if _, err := NewScraper(options); err == nil {
				t.Fatal("invalid explicit options accepted")
			}
		})
	}
}

func TestScrapeExtractsCompleteArticleUsingValidatedFinalURL(t *testing.T) {
	fixture := readFixture(t, "article.html")
	var gotHost, gotPath string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost, gotPath = r.Host, r.URL.Path
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Errorf("unexpected credential header forwarded")
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(fixture)
	}))
	defer server.Close()
	now := time.Date(2025, 2, 3, 4, 5, 6, 7, time.FixedZone("test", 3600))
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, func() time.Time { return now })
	article, err := trig.scraper.Scrape(context.Background(), articleURL)
	if err != nil {
		t.Fatal(err)
	}
	if article.RequestedURL != articleURL || article.FinalURL != articleURL || gotHost != "news.example" || gotPath != "/article" {
		t.Fatalf("URL contract: article=%+v Host=%q path=%q", article, gotHost, gotPath)
	}
	if article.FetchedAt.Location() != time.UTC || !article.FetchedAt.Equal(now) {
		t.Fatalf("FetchedAt=%v, want UTC representation of %v", article.FetchedAt, now)
	}
	for _, want := range []string{"twelve sailors", "engine stopped"} {
		if !strings.Contains(article.Text, want) {
			t.Errorf("article text omitted %q: %q", want, article.Text)
		}
	}
	for _, unwanted := range []string{"Subscribe", "Limited time offer", "must not appear"} {
		if strings.Contains(article.Text, unwanted) {
			t.Errorf("article text retained clutter %q", unwanted)
		}
	}
	if article.Title != "Harbor Rescue Report" || article.Language != "en" {
		t.Fatalf("metadata not extracted: title=%q language=%q", article.Title, article.Language)
	}
	if trig.dials.Load() != 1 || trig.lookups.Load() != 1 {
		t.Fatalf("dials=%d lookups=%d", trig.dials.Load(), trig.lookups.Load())
	}
}

func TestInjectedExtractorCauseIsWrappedSafely(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, "<html><body>bounded input</body></html>")
	}))
	defer server.Close()
	cause := errors.New("parser internal URL https://secret.example/?token=private")
	extractor := articleExtractor(func(io.Reader, *url.URL) (extractedArticle, error) { return extractedArticle{}, cause })
	trig := &scraperRig{server: server}
	trig.scraper = mustNewScraper(t, DefaultOptions(), resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	}), dialFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		local, err := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		return local, err
	}), extractor, nil, nil)
	_, err := trig.scraper.Scrape(context.Background(), "http://news.example/extract")
	assertScrapeError(t, err, ScrapeErrorExtraction, ScrapeStageExtract, 0)
	if !errors.Is(err, cause) || strings.Contains(err.Error(), "secret.example") {
		t.Fatalf("unsafe extractor error: %v", err)
	}
}

func TestScrapeRejectsEmptyReadabilityResult(t *testing.T) {
	body := readFixture(t, "no_article.html")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	article, err := trig.scraper.Scrape(context.Background(), articleURL)
	if err == nil {
		t.Fatalf("non-article fixture produced text: %q", article.Text)
	}
	assertScrapeError(t, err, ScrapeErrorExtraction, ScrapeStageExtract, 0)
}

func TestScrapeValidatesInitialURLBeforeNetwork(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("blocked URL reached server") }))
	defer server.Close()
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	for _, raw := range []string{"http://127.0.0.1/", "https://user:secret@news.example/", "ftp://news.example/"} {
		_, err := trig.scraper.Scrape(context.Background(), raw)
		assertScrapeError(t, err, ScrapeErrorSecurityRejected, ScrapeStageValidation, 0)
	}
	if trig.dials.Load() != 0 || trig.lookups.Load() != 0 {
		t.Fatalf("invalid URL caused lookup/dial: %d/%d", trig.lookups.Load(), trig.dials.Load())
	}
}

func TestTransportHasNoProxyCookieOrHTTP2EscapeHatches(t *testing.T) {
	scraper, err := NewScraper(DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := scraper.client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type %T", scraper.client.Transport)
	}
	if transport.Proxy != nil || !transport.DisableKeepAlives || transport.TLSNextProto == nil || len(transport.TLSNextProto) != 0 || transport.ForceAttemptHTTP2 || scraper.client.Jar != nil {
		t.Fatalf("transport permits alternate routing/state: %+v jar=%v", transport, scraper.client.Jar)
	}
	if transport.TLSClientConfig == nil || transport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("TLS verification is not enabled with a dedicated config")
	}
	if scraper.client.CheckRedirect == nil {
		t.Fatal("redirects must be handled by the policy-aware client")
	}
}

func TestRedirectsResolveRelativeAndAbsoluteURLsWithoutReferer(t *testing.T) {
	var requested []string
	var referers []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.Host+r.URL.RequestURI())
		referers = append(referers, r.Header.Get("Referer"))
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/middle", http.StatusFound)
			return
		}
		if r.URL.Path == "/middle" {
			http.Redirect(w, r, "http://final.example/article", http.StatusTemporaryRedirect)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, string(readFixture(t, "article.html")))
	}))
	defer server.Close()
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	article, err := trig.scraper.Scrape(context.Background(), "http://news.example/start?secret=not-forwarded")
	if err != nil {
		t.Fatal(err)
	}
	if article.FinalURL != "http://final.example/article" || len(requested) != 3 || trig.lookups.Load() != 3 || trig.dials.Load() != 3 {
		t.Fatalf("final=%q requests=%v DNS=%d dials=%d", article.FinalURL, requested, trig.lookups.Load(), trig.dials.Load())
	}
	for _, referer := range referers {
		if referer != "" {
			t.Fatalf("redirect forwarded Referer %q", referer)
		}
	}
}

func TestRedirectDestinationPolicyRejectsBeforeDestinationRequest(t *testing.T) {
	for _, destination := range []string{
		"http://127.0.0.1/private", "http://10.1.2.3/private", "http://169.254.169.254/latest", "http://metadata.google.internal/",
	} {
		t.Run(destination, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Redirect(w, r, destination, http.StatusFound)
			}))
			defer server.Close()
			trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
			_, err := trig.scraper.Scrape(context.Background(), "http://news.example/start")
			assertScrapeError(t, err, ScrapeErrorSecurityRejected, ScrapeStageValidation, 0)
			if requests.Load() != 1 || trig.dials.Load() != 1 {
				t.Fatalf("requests=%d dials=%d", requests.Load(), trig.dials.Load())
			}
		})
	}
}

func TestRedirectLimitZeroAndExcessRejectWithoutNextRequest(t *testing.T) {
	for _, test := range []struct {
		name      string
		max       int
		wantCalls int32
	}{
		{"zero", 0, 1}, {"one hop limit", 1, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Redirect(w, r, "/next", http.StatusFound)
			}))
			defer server.Close()
			options := DefaultOptions()
			options.MaxRedirects = test.max
			trig := newScraperRig(t, options, server, nil, nil, nil)
			_, err := trig.scraper.Scrape(context.Background(), "http://news.example/start")
			assertScrapeError(t, err, ScrapeErrorSecurityRejected, ScrapeStageFetch, 0)
			if requests.Load() != test.wantCalls {
				t.Fatalf("requests=%d want=%d", requests.Load(), test.wantCalls)
			}
		})
	}
}

func TestHTTPSDowngradeRejectedBeforeHTTPRequest(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, "http://plain.example/", http.StatusFound)
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	trig := newScraperRig(t, DefaultOptions(), server, nil, roots, nil)
	_, err := trig.scraper.Scrape(context.Background(), "https://example.com/start")
	assertScrapeError(t, err, ScrapeErrorSecurityRejected, ScrapeStageValidation, 0)
	if requests.Load() != 1 || trig.dials.Load() != 1 {
		t.Fatalf("requests=%d dials=%d", requests.Load(), trig.dials.Load())
	}
}

func TestRedirectHopMixedDNSRejectsBeforeSecondDial(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, "http://next.example/article", http.StatusFound)
	}))
	defer server.Close()
	var lookups atomic.Int32
	resolver := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		if lookups.Add(1) == 1 {
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("8.8.4.4"), netip.MustParseAddr("10.0.0.1")}, nil
	})
	trig := newScraperRig(t, DefaultOptions(), server, resolver, nil, nil)
	_, err := trig.scraper.Scrape(context.Background(), "http://news.example/start")
	assertScrapeError(t, err, ScrapeErrorSecurityRejected, ScrapeStageFetch, 0)
	if requests.Load() != 1 || trig.dials.Load() != 1 || lookups.Load() != 2 {
		t.Fatalf("requests=%d dials=%d lookups=%d", requests.Load(), trig.dials.Load(), lookups.Load())
	}
}

func TestDNSRebindingBetweenRedirectHopsRejectsBeforeSecondDial(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, "http://next.example/article", http.StatusFound)
	}))
	defer server.Close()
	var lookups atomic.Int32
	resolver := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		if lookups.Add(1) == 1 {
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		}
		return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
	})
	trig := newScraperRig(t, DefaultOptions(), server, resolver, nil, nil)
	_, err := trig.scraper.Scrape(context.Background(), "http://news.example/start")
	assertScrapeError(t, err, ScrapeErrorSecurityRejected, ScrapeStageFetch, 0)
	if requests.Load() != 1 || trig.dials.Load() != 1 || lookups.Load() != 2 {
		t.Fatalf("requests=%d dials=%d lookups=%d", requests.Load(), trig.dials.Load(), lookups.Load())
	}
}

func TestMixedDNSAnswerBlocksInitialRequestBeforeDial(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer server.Close()
	resolver := resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("10.0.0.1")}, nil
	})
	trig := newScraperRig(t, DefaultOptions(), server, resolver, nil, nil)
	_, err := trig.scraper.Scrape(context.Background(), "http://news.example/start")
	assertScrapeError(t, err, ScrapeErrorSecurityRejected, ScrapeStageFetch, 0)
	if requests.Load() != 0 || trig.dials.Load() != 0 {
		t.Fatalf("blocked DNS caused requests=%d dials=%d", requests.Load(), trig.dials.Load())
	}
}

func TestRedirectBodyIsCanceledAndClosedWithoutUnboundedDrain(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/start" {
			w.Header().Set("Location", "/final")
			w.WriteHeader(http.StatusFound)
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, strings.Repeat("redirect body ", 1024))
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write(readFixture(t, "article.html"))
	}))
	defer server.Close()
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	if _, err := trig.scraper.Scrape(context.Background(), "http://news.example/start"); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests=%d", requests.Load())
	}
}

func TestEveryHopUsesOneGlobalRequestDeadline(t *testing.T) {
	options := DefaultOptions()
	options.RequestTimeout = 180 * time.Millisecond
	options.HeaderTimeout = time.Second
	options.ReadTimeout = time.Second
	firstDeadline := make(chan time.Time, 1)
	secondDeadline := make(chan time.Time, 1)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		deadline, _ := r.Context().Deadline()
		if r.URL.Path == "/start" {
			firstDeadline <- deadline
			timer := time.NewTimer(100 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
				http.Redirect(w, r, "/wait", http.StatusFound)
			case <-r.Context().Done():
			}
			return
		}
		secondDeadline <- deadline
		<-r.Context().Done()
	}))
	defer server.Close()
	trig := newScraperRig(t, options, server, nil, nil, nil)
	start := time.Now()
	_, err := trig.scraper.Scrape(context.Background(), "http://news.example/start")
	elapsed := time.Since(start)
	assertScrapeError(t, err, ScrapeErrorTimeout, ScrapeStageFetch, 0)
	first, second := <-firstDeadline, <-secondDeadline
	if !first.Equal(second) {
		t.Fatalf("redirect reset deadline: first=%v second=%v", first, second)
	}
	if requests.Load() != 2 || elapsed > 500*time.Millisecond {
		t.Fatalf("requests=%d elapsed=%v", requests.Load(), elapsed)
	}
}

func TestHeaderAndTotalBodyBudgetsTimeout(t *testing.T) {
	t.Run("headers", func(t *testing.T) {
		options := DefaultOptions()
		options.HeaderTimeout = 60 * time.Millisecond
		options.RequestTimeout = time.Second
		server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
		defer server.Close()
		trig := newScraperRig(t, options, server, nil, nil, nil)
		_, err := trig.scraper.Scrape(context.Background(), "http://news.example/slow-headers")
		assertScrapeError(t, err, ScrapeErrorTimeout, ScrapeStageFetch, 0)
	})
	t.Run("body trickle", func(t *testing.T) {
		options := DefaultOptions()
		options.ReadTimeout = 70 * time.Millisecond
		options.RequestTimeout = time.Second
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			_, _ = io.WriteString(w, "<html>")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		defer server.Close()
		trig := newScraperRig(t, options, server, nil, nil, nil)
		_, err := trig.scraper.Scrape(context.Background(), "http://news.example/trickle")
		assertScrapeError(t, err, ScrapeErrorTimeout, ScrapeStageFetch, 0)
	})
}

func TestParentCancellationDuringRequestIsOperational(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := trig.scraper.Scrape(ctx, "http://news.example/cancel")
		result <- err
	}()
	<-started
	cancel()
	assertScrapeError(t, <-result, ScrapeErrorCanceled, ScrapeStageFetch, 0)
}

func TestParentCancellationAndDialCauseArePreserved(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("canceled request reached server") }))
	defer server.Close()
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := trig.scraper.Scrape(ctx, articleURL)
	assertScrapeError(t, err, ScrapeErrorCanceled, ScrapeStageFetch, 0)
	if trig.dials.Load() != 0 {
		t.Fatalf("canceled request dialed %d times", trig.dials.Load())
	}

	cause := errors.New("private address https://secret.example/?token=hidden")
	failing := mustNewScraper(t, DefaultOptions(), resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	}), dialFunc(func(context.Context, string, string) (net.Conn, error) { return nil, cause }), nil, nil, nil)
	_, err = failing.Scrape(context.Background(), articleURL)
	var typed *ScrapeError
	if !errors.As(err, &typed) || !errors.Is(err, cause) || strings.Contains(err.Error(), "secret.example") || strings.Contains(err.Error(), "token=hidden") {
		t.Fatalf("cause/safe error contract: %#v %v", typed, err)
	}
}

func TestInvalidTLSCertificateCauseAndOriginalSNI(t *testing.T) {
	var serverName atomic.Value
	var protocol atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverName.Store(r.TLS.ServerName)
		protocol.Store(int32(r.ProtoMajor))
		w.Header().Set("Content-Type", "text/html")
		_, _ = io.WriteString(w, string(readFixture(t, "article.html")))
	}))
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	trig := newScraperRig(t, DefaultOptions(), server, nil, roots, nil)
	if _, err := trig.scraper.Scrape(context.Background(), "https://example.com/secure"); err != nil {
		t.Fatal(err)
	}
	if got := serverName.Load(); got != "example.com" {
		t.Fatalf("TLS SNI=%v; want original request host", got)
	}
	if got := protocol.Load(); got != 1 {
		t.Fatalf("HTTP protocol version=%d; want HTTP/1.x", got)
	}

	untrusted := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	_, err := untrusted.scraper.Scrape(context.Background(), "https://example.com/secure")
	assertScrapeError(t, err, ScrapeErrorNetwork, ScrapeStageFetch, 0)
	var verificationErr *tls.CertificateVerificationError
	if !errors.As(err, &verificationErr) {
		t.Fatalf("certificate verification cause unavailable: %T %v", err, err)
	}
}

func TestTLSHandshakeBudgetExpires(t *testing.T) {
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	server.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		started <- struct{}{}
		<-release
		return nil, nil
	}}
	server.StartTLS()
	defer server.Close()
	options := DefaultOptions()
	options.TLSTimeout = 60 * time.Millisecond
	options.RequestTimeout = time.Second
	trig := newScraperRig(t, options, server, nil, nil, nil)
	result := make(chan error, 1)
	go func() {
		_, err := trig.scraper.Scrape(context.Background(), "https://example.com/handshake")
		result <- err
	}()
	<-started
	assertScrapeError(t, <-result, ScrapeErrorTimeout, ScrapeStageFetch, 0)
	close(release)
}

func TestHTTPStatusesAndUnsupportedResponses(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusMultipleChoices} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "not HTML")
			}))
			defer server.Close()
			trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
			_, err := trig.scraper.Scrape(context.Background(), "http://news.example/status")
			assertScrapeError(t, err, ScrapeErrorHTTPStatus, ScrapeStageFetch, status)
		})
	}
}

func TestMIMEParsingBoundedSniffAndUnsupportedEncoding(t *testing.T) {
	for _, test := range []struct {
		name, contentType, encoding string
		body                        string
		wantKind                    ScrapeErrorKind
	}{
		{"missing type HTML sniff", "", "", string(readFixture(t, "article.html")), ""},
		{"HTML beyond bounded sniff", "", "", strings.Repeat(" ", contentSniffBytes+1) + string(readFixture(t, "article.html")), ScrapeErrorUnsupported},
		{"binary MIME", "application/octet-stream", "", "<html><body>not accepted</body></html>", ScrapeErrorUnsupported},
		{"malformed MIME", "text/html; charset=\"unterminated", "", "<html></html>", ScrapeErrorUnsupported},
		{"unknown charset", "text/html; charset=not-a-real-charset", "", "<html><body>article</body></html>", ScrapeErrorUnsupported},
		{"unknown content encoding", "text/html", "br", "compressed", ScrapeErrorUnsupported},
		{"stacked content encoding", "text/html", "gzip, br", "compressed", ScrapeErrorUnsupported},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.contentType != "" {
					w.Header().Set("Content-Type", test.contentType)
				}
				if test.encoding != "" {
					w.Header().Set("Content-Encoding", test.encoding)
				}
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
			article, err := trig.scraper.Scrape(context.Background(), "http://news.example/mime")
			if test.wantKind == "" {
				if err != nil || !strings.Contains(article.Text, "twelve sailors") {
					t.Fatalf("sniffed HTML article=%+v err=%v", article, err)
				}
				return
			}
			assertScrapeError(t, err, test.wantKind, ScrapeStageFetch, 0)
		})
	}
}

func TestCharsetExpansionBeyondWireLimitStillReachesExtractor(t *testing.T) {
	prefix := []byte("<html><body><article>")
	suffix := []byte("</article></body></html>")
	wireBody := append(append(append([]byte(nil), prefix...), bytes.Repeat([]byte{0xe9}, 40)...), suffix...)
	wantUnicode := strings.Repeat("é", 40)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=iso-8859-1")
		_, _ = w.Write(wireBody)
	}))
	defer server.Close()
	options := DefaultOptions()
	options.MaxBodyBytes = int64(len(wireBody))
	var extractorSawExpandedUnicode bool
	extractor := articleExtractor(func(input io.Reader, _ *url.URL) (extractedArticle, error) {
		decoded, err := io.ReadAll(input)
		if err != nil {
			return extractedArticle{}, err
		}
		extractorSawExpandedUnicode = len(decoded) > len(wireBody) && bytes.Contains(decoded, []byte(wantUnicode))
		return extractedArticle{text: "decoded article"}, nil
	})
	trig := &scraperRig{server: server}
	trig.scraper = mustNewScraper(t, options, resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	}), dialFunc(func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}), extractor, nil, nil)
	article, err := trig.scraper.Scrape(context.Background(), "http://news.example/latin1")
	if err != nil {
		t.Fatalf("valid within-limit Latin-1 article failed: %v", err)
	}
	if article.Text != "decoded article" || !extractorSawExpandedUnicode {
		t.Fatalf("article=%+v extractorSawExpandedUnicode=%v", article, extractorSawExpandedUnicode)
	}
}

func TestExactMaximumHTMLReachesInjectedExtractorAndPlusOneFails(t *testing.T) {
	for _, size := range []int{64, 65} {
		t.Run(fmt.Sprintf("wire-bytes-%d", size), func(t *testing.T) {
			wireBody := exactHTML(size)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(wireBody)
			}))
			defer server.Close()
			options := DefaultOptions()
			options.MaxBodyBytes = 64
			var extractorInput []byte
			extractor := articleExtractor(func(input io.Reader, _ *url.URL) (extractedArticle, error) {
				var err error
				extractorInput, err = io.ReadAll(input)
				return extractedArticle{text: "useful text"}, err
			})
			trig := &scraperRig{server: server}
			trig.scraper = mustNewScraper(t, options, resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
			}), dialFunc(func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}), extractor, nil, nil)
			article, err := trig.scraper.Scrape(context.Background(), "http://news.example/exact-boundary")
			if size == 64 {
				if err != nil || article.Text != "useful text" || !bytes.Equal(extractorInput, wireBody) {
					t.Fatalf("exact-limit article=%+v extractor bytes=%d err=%v", article, len(extractorInput), err)
				}
				return
			}
			assertScrapeError(t, err, ScrapeErrorBodyTooLarge, ScrapeStageFetch, 0)
			if len(extractorInput) != 0 {
				t.Fatalf("oversized response reached extractor with %d bytes", len(extractorInput))
			}
		})
	}
}

func TestResponseBodyReadLimitIsCumulativeAcrossSniffAndRead(t *testing.T) {
	for _, test := range []struct {
		name        string
		contentType string
		bodySize    int
		html        bool
		wantError   bool
	}{
		{"exact small limit preserves sniff prefix", "", 64, true, false},
		{"small limit plus one", "", 65, true, true},
		{"before sniff buffer boundary", "", 511, true, true},
		{"at sniff buffer boundary", "", 512, true, true},
		{"past sniff buffer boundary", "", 513, true, true},
		{"sniff overflow wins over unsupported prefix", "", 100, false, true},
		{"explicit MIME uses same limit", "text/html", 513, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			bodyBytes := bytes.Repeat([]byte("x"), test.bodySize)
			if test.html {
				copy(bodyBytes, []byte("<html><body>"))
			}
			body := &countingReadCloser{reader: bytes.NewReader(bodyBytes)}
			options := DefaultOptions()
			options.MaxBodyBytes = 64
			scraper := mustNewScraper(t, options, nil, nil, nil, nil, nil)
			header := make(http.Header)
			if test.contentType != "" {
				header.Set("Content-Type", test.contentType)
			}
			response := &http.Response{Header: header, ContentLength: -1, Body: body}
			got, err := scraper.readBody(context.Background(), func() {}, response)
			if test.wantError {
				assertScrapeError(t, err, ScrapeErrorBodyTooLarge, ScrapeStageFetch, 0)
				if len(got) != 0 {
					t.Fatalf("overflow returned %d body bytes", len(got))
				}
			} else if err != nil || !bytes.Equal(got, bodyBytes) {
				t.Fatalf("exact-limit body=%d err=%v", len(got), err)
			}
			if body.readBytes > int(options.MaxBodyBytes)+1 {
				t.Fatalf("response body read %d bytes; limit is %d", body.readBytes, options.MaxBodyBytes+1)
			}
			closeResponse(response)
			if !body.closed {
				t.Fatal("wrapped response body did not close original body")
			}
		})
	}
}

type countingReadCloser struct {
	reader    *bytes.Reader
	readBytes int
	closed    bool
}

func (body *countingReadCloser) Read(data []byte) (int, error) {
	n, err := body.reader.Read(data)
	body.readBytes += n
	return n, err
}

func (body *countingReadCloser) Close() error {
	body.closed = true
	return nil
}

func TestMIMECharsetIsDecoded(t *testing.T) {
	body := []byte("<!doctype html><html lang=\"en\"><body><article><h1>Food Notes</h1><p>The caf\xe9 opened near the station with fresh pastries and coffee for commuters.</p><p>Residents praised the new bakery for its warm bread and friendly service every morning.</p><p>Its owners plan to hire local students and source fruit from nearby farms this summer.</p><p>The opening celebration drew families from across the neighborhood throughout the day.</p></article></body></html>")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=iso-8859-1")
		_, _ = w.Write(body)
	}))
	defer server.Close()
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	article, err := trig.scraper.Scrape(context.Background(), "http://news.example/charset")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(article.Text, "café") {
		t.Fatalf("declared charset was not decoded: %q", article.Text)
	}
}

func TestBodyLimitExactMaxMaxPlusOneAndChunked(t *testing.T) {
	for _, test := range []struct {
		name      string
		extra     int
		chunked   bool
		wantError bool
	}{
		{"exact max", 0, false, false},
		{"max plus one", 1, false, true},
		{"chunked max plus one", 1, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const maxBytes = 256
			body := exactHTML(maxBytes + test.extra)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				if test.chunked {
					w.(http.Flusher).Flush()
				}
				_, _ = w.Write(body)
			}))
			defer server.Close()
			options := DefaultOptions()
			options.MaxBodyBytes = maxBytes
			trig := newScraperRig(t, options, server, nil, nil, nil)
			article, err := trig.scraper.Scrape(context.Background(), "http://news.example/limited")
			if test.wantError {
				assertScrapeError(t, err, ScrapeErrorBodyTooLarge, ScrapeStageFetch, 0)
			} else if err != nil || article.Text == "" {
				t.Fatalf("exact max body article=%+v err=%v", article, err)
			}
		})
	}
}

func TestAutomaticGzipDecodingProducesArticleText(t *testing.T) {
	compressed := gzipBytes(t, readFixture(t, "article.html"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			t.Errorf("transport did not negotiate automatic gzip: %q", r.Header.Get("Accept-Encoding"))
		}
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed)
	}))
	defer server.Close()
	trig := newScraperRig(t, DefaultOptions(), server, nil, nil, nil)
	article, err := trig.scraper.Scrape(context.Background(), "http://news.example/gzip")
	if err != nil || !strings.Contains(article.Text, "twelve sailors") {
		t.Fatalf("gzip article=%+v err=%v", article, err)
	}
}

func TestGzipExpansionAndCorruptionAreBounded(t *testing.T) {
	large := bytes.Repeat([]byte("article expansion "), 200)
	compressed := gzipBytes(t, append([]byte("<html><body><article><p>"), append(large, []byte("</p></article></body></html>")...)...))
	for _, test := range []struct {
		name string
		body []byte
		want ScrapeErrorKind
	}{
		{"decompressed expansion", compressed, ScrapeErrorBodyTooLarge},
		{"corrupt gzip", []byte("not a gzip stream"), ScrapeErrorNetwork},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				w.Header().Set("Content-Encoding", "gzip")
				_, _ = w.Write(test.body)
			}))
			defer server.Close()
			options := DefaultOptions()
			options.MaxBodyBytes = 256
			trig := newScraperRig(t, options, server, nil, nil, nil)
			_, err := trig.scraper.Scrape(context.Background(), "http://news.example/gzip")
			assertScrapeError(t, err, test.want, ScrapeStageFetch, 0)
		})
	}
}

func TestBodiesCloseAcrossSuccessRedirectAndErrors(t *testing.T) {
	for _, outcome := range []string{"success", "redirect", "status", "mime", "large", "empty extraction"} {
		t.Run(outcome, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch outcome {
				case "redirect":
					if r.URL.Path == "/start" {
						http.Redirect(w, r, "/done", http.StatusFound)
						return
					}
					w.Header().Set("Content-Type", "text/html")
					_, _ = w.Write(readFixture(t, "article.html"))
				case "status":
					w.WriteHeader(http.StatusNotFound)
				case "mime":
					w.Header().Set("Content-Type", "application/pdf")
					_, _ = io.WriteString(w, "pdf")
				case "large":
					w.Header().Set("Content-Type", "text/html")
					_, _ = io.WriteString(w, strings.Repeat("x", 300))
				case "empty extraction":
					w.Header().Set("Content-Type", "text/html")
					_, _ = w.Write(readFixture(t, "no_article.html"))
				default:
					w.Header().Set("Content-Type", "text/html")
					_, _ = w.Write(readFixture(t, "article.html"))
				}
			}))
			defer server.Close()
			options := DefaultOptions()
			if outcome == "large" {
				options.MaxBodyBytes = 100
			}
			trig := newScraperRig(t, options, server, nil, nil, nil)
			path := "/done"
			if outcome == "redirect" {
				path = "/start"
			}
			_, _ = trig.scraper.Scrape(context.Background(), "http://news.example"+path)
			waitForConnCloses(t, trig)
		})
	}
}

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func assertScrapeError(t *testing.T, err error, kind ScrapeErrorKind, stage ScrapeStage, status int) *ScrapeError {
	t.Helper()
	var scrapeErr *ScrapeError
	if !errors.As(err, &scrapeErr) {
		t.Fatalf("error %T %v is not ScrapeError", err, err)
	}
	if scrapeErr.Kind != kind || scrapeErr.Stage != stage || scrapeErr.HTTPStatus != status {
		t.Fatalf("ScrapeError=%+v want kind=%s stage=%s status=%d", scrapeErr, kind, stage, status)
	}
	if strings.Contains(scrapeErr.Error(), "news.example") || strings.Contains(scrapeErr.Error(), "secret") {
		t.Fatalf("unsafe error string %q", scrapeErr.Error())
	}
	return scrapeErr
}

func exactHTML(size int) []byte {
	prefix, suffix := "<html><body><article><p>", "</p></article></body></html>"
	padding := size - len(prefix) - len(suffix)
	if padding < 1 {
		panic("test HTML size too small")
	}
	return []byte(prefix + strings.Repeat("x", padding) + suffix)
}

func gzipBytes(t *testing.T, body []byte) []byte {
	t.Helper()
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(body); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return compressed.Bytes()
}

type trackedConn struct {
	net.Conn
	closed *atomic.Int32
	notify chan struct{}
	once   atomic.Bool
}

func (c *trackedConn) Close() error {
	if c.once.CompareAndSwap(false, true) {
		c.closed.Add(1)
		select {
		case c.notify <- struct{}{}:
		default:
		}
	}
	return c.Conn.Close()
}

type scraperRig struct {
	scraper *Scraper
	server  *httptest.Server
	lookups atomic.Int32
	dials   atomic.Int32
	closed  atomic.Int32
	notify  chan struct{}
}

func waitForConnCloses(t *testing.T, rig *scraperRig) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for rig.closed.Load() < rig.dials.Load() {
		select {
		case <-rig.notify:
		case <-deadline.C:
			t.Fatalf("connections not closed: closes=%d dials=%d", rig.closed.Load(), rig.dials.Load())
		}
	}
	if rig.closed.Load() == 0 {
		t.Fatal("expected at least one connection close")
	}
}

func newScraperRig(t *testing.T, options ScraperOptions, server *httptest.Server, resolver addressResolver, roots *x509.CertPool, now func() time.Time) *scraperRig {
	t.Helper()
	rig := &scraperRig{server: server, notify: make(chan struct{}, 16)}
	if resolver == nil {
		resolver = resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			rig.lookups.Add(1)
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		})
	} else {
		original := resolver
		resolver = resolverFunc(func(ctx context.Context, network, host string) ([]netip.Addr, error) {
			rig.lookups.Add(1)
			return original.LookupNetIP(ctx, network, host)
		})
	}
	dialer := dialFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
		rig.dials.Add(1)
		literalHost, _, err := net.SplitHostPort(address)
		if err != nil || net.ParseIP(literalHost) == nil {
			return nil, fmt.Errorf("test dialer expected validated literal address: %q", address)
		}
		local, err := (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
		if err != nil {
			return nil, err
		}
		return &trackedConn{Conn: local, closed: &rig.closed, notify: rig.notify}, nil
	})
	var err error
	rig.scraper, err = newScraper(options, resolver, dialer, nil, now, roots)
	if err != nil {
		t.Fatal(err)
	}
	return rig
}

func mustNewScraper(t *testing.T, options ScraperOptions, resolver addressResolver, dialer contextDialer, extractor articleExtractor, now func() time.Time, roots *x509.CertPool) *Scraper {
	t.Helper()
	scraper, err := newScraper(options, resolver, dialer, extractor, now, roots)
	if err != nil {
		t.Fatal(err)
	}
	return scraper
}
