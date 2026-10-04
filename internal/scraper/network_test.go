package scraper

import (
	"context"
	"crypto/x509"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestValidateURLPolicy(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{"https implicit", "https://example.com/article?q=private", true},
		{"mapped public literal", "https://[::ffff:8.8.8.8]/", true},
		{"http standard port", "http://example.com:80/", true},
		{"https standard port", "https://example.com:443/", true},
		{"alternate port", "https://example.com:8443/", false},
		{"leading zero standard port", "https://example.com:0443/", false},
		{"empty explicit port", "https://example.com:/", false},
		{"nonnumeric port", "https://example.com:abc/", false},
		{"credentials", "https://user:pass@example.com/", false},
		{"relative", "//example.com/path", false},
		{"opaque", "https:example.com/path", false},
		{"unsupported scheme", "ftp://example.com/", false},
		{"missing hostname", "https:///path", false},
		{"metadata hostname", "http://metadata.google.internal./", false},
		{"metadata AWS hostname", "http://instance-data/", false},
		{"metadata AWS internal hostname", "http://instance-data.ec2.internal/", false},
		{"percent host", "http://exa%6dple.com/", false},
		{"invalid numeric host", "http://0177.0.0.1/", false},
		{"short numeric host", "http://127.1/", false},
		{"hex numeric host", "http://0x7f000001/", false},
		{"loopback literal", "http://127.0.0.1/", false},
		{"private literal", "https://10.1.2.3/", false},
		{"mapped private", "http://[::ffff:10.1.2.3]/", false},
		{"legacy hex IPv4 one component", "http://0x7f000001/", false},
		{"legacy uppercase hex IPv4", "http://0X7F000001/", false},
		{"legacy hex IPv4 components", "http://0x7f.0.0.1/", false},
		{"legacy mixed hex IPv4 component", "http://127.0x0.0.1/", false},
		{"legacy two-part hex IPv4", "http://0x7f.1/", false},
		{"legacy public-looking hex IPv4", "http://0x08080808/", false},
		{"legacy uppercase trailing-dot hex IPv4", "http://0X7F000001./", false},
		{"legacy trailing-dot mixed hex IPv4", "http://127.0X0.0.1./", false},
		{"IPv6 zone", "http://[fe80::1%25eth0]/", false},
		{"bracket malformed", "http://[::1]oops/", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := validateURL(test.raw)
			if got := err == nil; got != test.want {
				t.Fatalf("validateURL(%q) accepted=%v, want %v (err=%v)", test.raw, got, test.want, err)
			}
			if err != nil && !errors.Is(err, ErrSecurityRejected) {
				t.Fatalf("rejection does not preserve security classification: %v", err)
			}
		})
	}
}

func TestPublicAddressPolicy(t *testing.T) {
	tests := []struct {
		address string
		want    bool
	}{
		{"8.8.8.8", true}, {"1.1.1.1", true},
		{"0.1.2.3", false}, {"10.1.2.3", false}, {"100.64.0.1", false},
		{"127.0.0.1", false}, {"169.254.169.254", false}, {"169.254.170.2", false},
		{"100.100.100.200", false}, {"168.63.129.16", false}, {"172.31.0.1", false},
		{"168.63.129.15", true}, {"168.63.129.16", false}, {"168.63.129.17", true}, {"168.63.0.1", true},
		{"192.0.0.8", false}, {"192.0.2.1", false}, {"192.31.196.1", false},
		{"192.52.193.1", false}, {"192.88.99.1", false}, {"192.175.48.1", false},
		{"198.18.0.1", false}, {"198.51.100.1", false}, {"203.0.113.1", false},
		{"224.0.0.1", false}, {"240.0.0.1", false}, {"255.255.255.255", false},
		{"::", false}, {"::1", false}, {"fc00::1", false}, {"fe80::1", false},
		{"ff02::1", false}, {"2001:db8::1", false}, {"2001::1", false}, {"3fff::1", false},
		{"2002:0808:0808::1", false}, {"64:ff9b::808:808", false},
		{"2001:4860:4860::8888", true}, {"2606:4700:4700::1111", true},
	}
	for _, test := range tests {
		t.Run(test.address, func(t *testing.T) {
			got := isPublicAddress(netip.MustParseAddr(test.address))
			if got != test.want {
				t.Fatalf("isPublicAddress(%s)=%v, want %v", test.address, got, test.want)
			}
		})
	}
}

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}

type dialFunc func(context.Context, string, string) (net.Conn, error)

func (f dialFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
}

func TestSafeDialerPinsApprovedAddressAndSharesDeadline(t *testing.T) {
	start := time.Now()
	deadline := start.Add(3 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	var resolvedContextDeadline, dialContextDeadline time.Time
	var dialed string
	client, server := net.Pipe()
	defer server.Close()
	resolver := resolverFunc(func(got context.Context, _, host string) ([]netip.Addr, error) {
		if host != "news.example" {
			t.Fatalf("resolver host=%q", host)
		}
		resolvedContextDeadline, _ = got.Deadline()
		return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
	})
	dialer := dialFunc(func(got context.Context, network, address string) (net.Conn, error) {
		dialContextDeadline, _ = got.Deadline()
		dialed = network + ":" + address
		return client, nil
	})
	connect := newSafeDialer(5*time.Second, resolver, dialer)
	conn, err := connect(ctx, "tcp", "news.example:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if dialed != "tcp:8.8.8.8:443" {
		t.Fatalf("dialed %q, want vetted literal", dialed)
	}
	if !resolvedContextDeadline.Equal(deadline) || !dialContextDeadline.Equal(deadline) {
		t.Fatalf("deadline changed across resolution/dial: resolve=%v dial=%v want=%v", resolvedContextDeadline, dialContextDeadline, deadline)
	}
}

func TestAzureWireServerAddressBoundary(t *testing.T) {
	for _, octet := range []string{"168.63.129.15", "168.63.129.17", "168.63.0.1"} {
		t.Run(octet, func(t *testing.T) {
			if _, err := validateURL("http://" + octet + "/"); err != nil {
				t.Fatalf("public adjacent address rejected: %v", err)
			}
			client, server := net.Pipe()
			defer server.Close()
			connect := newSafeDialer(time.Second, nil, dialFunc(func(_ context.Context, _, address string) (net.Conn, error) {
				if address != octet+":80" {
					t.Fatalf("dialed %q", address)
				}
				return client, nil
			}))
			conn, err := connect(context.Background(), "tcp", octet+":80")
			if err != nil {
				t.Fatalf("public adjacent address could not dial: %v", err)
			}
			conn.Close()
		})
	}
	if _, err := validateURL("http://168.63.129.16/"); err == nil {
		t.Fatal("Azure WireServer endpoint accepted")
	}
	dials := 0
	connect := newSafeDialer(time.Second, nil, dialFunc(func(context.Context, string, string) (net.Conn, error) {
		dials++
		return nil, errors.New("unexpected")
	}))
	if _, err := connect(context.Background(), "tcp", "168.63.129.16:80"); err == nil || dials != 0 {
		t.Fatalf("metadata endpoint err=%v dials=%d", err, dials)
	}
}

func TestHexLikeDNSNamesResolveThroughVettedLiteral(t *testing.T) {
	hosts := []string{
		"0x.org", "100x.com", "0xdata.com", "fox0x.com", "node-0x1a.example.com",
		"0xdeadbeef.example.com", "0x123.example.com", "0XDEADBEEF.Example.COM.", "0X.Org.",
	}
	for _, host := range hosts {
		t.Run(host, func(t *testing.T) {
			if _, err := validateURL("https://" + host + "/"); err != nil {
				t.Fatalf("valid DNS hostname rejected: %v", err)
			}
			client, server := net.Pipe()
			defer server.Close()
			lookups, dials := 0, 0
			connect := newSafeDialer(time.Second,
				resolverFunc(func(_ context.Context, _, gotHost string) ([]netip.Addr, error) {
					lookups++
					if !strings.EqualFold(gotHost, strings.TrimSuffix(host, ".")) && gotHost != host {
						t.Fatalf("resolver host %q", gotHost)
					}
					return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
				}),
				dialFunc(func(_ context.Context, _, address string) (net.Conn, error) {
					dials++
					if address != "8.8.8.8:443" {
						t.Fatalf("unvetted dial address %q", address)
					}
					return client, nil
				}),
			)
			conn, err := connect(context.Background(), "tcp", net.JoinHostPort(host, "443"))
			if err != nil {
				t.Fatalf("public DNS answer rejected: %v", err)
			}
			conn.Close()
			if lookups != 1 || dials != 1 {
				t.Fatalf("lookups=%d dials=%d", lookups, dials)
			}
		})
	}
}

func TestHexLikeDNSNamesStillRejectPrivateOrMixedDNS(t *testing.T) {
	for _, addresses := range [][]netip.Addr{
		{netip.MustParseAddr("10.0.0.1")},
		{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("10.0.0.1")},
	} {
		dials := 0
		connect := newSafeDialer(time.Second,
			resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) { return addresses, nil }),
			dialFunc(func(context.Context, string, string) (net.Conn, error) { dials++; return nil, errors.New("unexpected") }),
		)
		_, err := connect(context.Background(), "tcp", "0xdeadbeef.example.com:443")
		if err == nil || !errors.Is(err, ErrSecurityRejected) || dials != 0 {
			t.Fatalf("addresses=%v err=%v dials=%d", addresses, err, dials)
		}
	}
}

func TestLegacyEncodedIPv4FormsRejectBeforeDNSOrDial(t *testing.T) {
	hosts := []string{
		"0x7f000001", "0X7F000001", "0x7f.0.0.1", "127.0x0.0.1", "0x7f.1", "0x08080808",
		"0X7F000001.", "0X7F.0.0.1.", "127.0X0.0.1.", "0X7F.1.", "0X08080808.",
	}
	for _, host := range hosts {
		t.Run(host, func(t *testing.T) {
			if _, err := validateURL("http://" + host + "/"); err == nil {
				t.Fatal("legacy encoded IPv4 form accepted as URL hostname")
			}
			lookups, dials := 0, 0
			connect := newSafeDialer(time.Second,
				resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
					lookups++
					return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
				}),
				dialFunc(func(context.Context, string, string) (net.Conn, error) { dials++; return nil, errors.New("unexpected") }),
			)
			_, err := connect(context.Background(), "tcp", net.JoinHostPort(host, "80"))
			if err == nil || lookups != 0 || dials != 0 {
				t.Fatalf("err=%v lookups=%d dials=%d", err, lookups, dials)
			}
		})
	}
}

func TestSafeDialerRejectsBlockedOrMixedDNSBeforeDial(t *testing.T) {
	for _, addresses := range [][]netip.Addr{
		{netip.MustParseAddr("127.0.0.1")},
		{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("10.0.0.1")},
		{},
	} {
		dials := 0
		connect := newSafeDialer(time.Second,
			resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) { return addresses, nil }),
			dialFunc(func(context.Context, string, string) (net.Conn, error) { dials++; return nil, errors.New("unexpected") }),
		)
		_, err := connect(context.Background(), "tcp", "news.example:443")
		if err == nil || dials != 0 || !errors.Is(err, ErrSecurityRejected) {
			t.Fatalf("addresses=%v err=%v dials=%d; want safe rejection before dialing", addresses, err, dials)
		}
	}
}

func TestSafeDialerRejectsBlockedLiteralWithoutDial(t *testing.T) {
	dials := 0
	connect := newSafeDialer(time.Second, nil, dialFunc(func(context.Context, string, string) (net.Conn, error) {
		dials++
		return nil, errors.New("unexpected dial")
	}))
	_, err := connect(context.Background(), "tcp", "127.0.0.1:443")
	if err == nil || !errors.Is(err, ErrSecurityRejected) || dials != 0 {
		t.Fatalf("blocked literal err=%v dials=%d; want rejection before dial", err, dials)
	}
}

func TestSafeDialerChecksLiteralIPWithoutResolver(t *testing.T) {
	client, server := net.Pipe()
	defer server.Close()
	calls := 0
	connect := newSafeDialer(time.Second,
		resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			t.Fatal("literal triggered DNS")
			return nil, nil
		}),
		dialFunc(func(_ context.Context, _, address string) (net.Conn, error) {
			calls++
			if address != "8.8.8.8:443" {
				t.Fatalf("dial address %q", address)
			}
			return client, nil
		}),
	)
	conn, err := connect(context.Background(), "tcp", "8.8.8.8:443")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if calls != 1 {
		t.Fatalf("dial calls=%d, want one", calls)
	}
}

func TestSafeDialerResolvesFreshAndDoesNotRedialBlockedRebinding(t *testing.T) {
	lookups, dials := 0, 0
	client, server := net.Pipe()
	defer server.Close()
	connect := newSafeDialer(time.Second,
		resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			lookups++
			if lookups == 1 {
				return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
			}
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		}),
		dialFunc(func(_ context.Context, _, address string) (net.Conn, error) {
			dials++
			if address != "8.8.8.8:443" {
				t.Fatalf("unvalidated address dialed: %s", address)
			}
			return client, nil
		}),
	)
	first, err := connect(context.Background(), "tcp", "news.example:443")
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	_, secondErr := connect(context.Background(), "tcp", "news.example:443")
	if secondErr == nil || !errors.Is(secondErr, ErrSecurityRejected) || lookups != 2 || dials != 1 {
		t.Fatalf("second lookup err=%v lookups=%d dials=%d", secondErr, lookups, dials)
	}
}

func TestSafeDialerPreservesNetworkCauseAndContextClassification(t *testing.T) {
	cause := errors.New("secret URL https://private.example/?token=raw")
	connect := newSafeDialer(time.Second,
		resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
		}),
		dialFunc(func(context.Context, string, string) (net.Conn, error) { return nil, cause }),
	)
	_, err := connect(context.Background(), "tcp", "news.example:443")
	if !errors.Is(err, cause) || strings.Contains(err.Error(), "private.example") || strings.Contains(err.Error(), "token=raw") {
		t.Fatalf("network cause wrapping/safe message incorrect: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = connect(canceled, "tcp", "news.example:443")
	if !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancellation classification=%v", err)
	}
	deadline, stop := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop()
	_, err = connect(deadline, "tcp", "news.example:443")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline classification=%v", err)
	}
}

func TestScrapeErrorIsSafeAndUnwrapsCause(t *testing.T) {
	cause := errors.New("raw URL https://private.example/path?token=hidden")
	scrapeErr := &ScrapeError{Kind: ScrapeErrorNetwork, Stage: ScrapeStageFetch, Message: "connection failed", Cause: cause}
	if !errors.Is(scrapeErr, cause) {
		t.Fatal("Unwrap did not preserve cause")
	}
	if strings.Contains(scrapeErr.Error(), "private.example") || strings.Contains(scrapeErr.Error(), "token=hidden") {
		t.Fatalf("Error exposed cause: %q", scrapeErr.Error())
	}
}

func TestScrapeErrorAsPreservesCertificateCause(t *testing.T) {
	cause := x509.CertificateInvalidError{Reason: x509.Expired}
	scrapeErr := &ScrapeError{Kind: ScrapeErrorNetwork, Stage: ScrapeStageFetch, Cause: cause}
	var got x509.CertificateInvalidError
	if !errors.As(scrapeErr, &got) || got.Reason != x509.Expired {
		t.Fatalf("certificate cause not preserved through errors.As: %#v", got)
	}
}

func TestScrapeErrorUnknownFieldsCannotInjectUnsafeText(t *testing.T) {
	err := &ScrapeError{Kind: ScrapeErrorKind("https://private.example/?token=raw"), Stage: ScrapeStage("url=https://private.example"), Message: "private URL"}
	if strings.Contains(err.Error(), "private.example") || strings.Contains(err.Error(), "token=raw") {
		t.Fatalf("unsafe classification serialized: %q", err.Error())
	}
}

func TestURLParserDoesNotChangeCallerURLAndSplitAuthorityIsRejected(t *testing.T) {
	raw := "https://news.example/path?q=keep#fragment"
	before := raw
	parsed, err := validateURL(raw)
	if err != nil {
		t.Fatal(err)
	}
	if raw != before || parsed.RawQuery != "q=keep" || parsed.Fragment != "fragment" {
		t.Fatalf("input mutated or parts lost: %q %#v", raw, parsed)
	}
	for _, raw := range []string{"https:////evil.example", "https://[2001:4860:4860::8888]:443@evil.example/"} {
		if _, err := url.Parse(raw); err == nil {
			// Parser acceptance is not policy acceptance; authority validation owns it.
		}
		if _, err := validateURL(raw); err == nil {
			t.Errorf("accepted ambiguous URL %q", raw)
		}
	}
}
