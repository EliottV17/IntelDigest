package scraper

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

const defaultConnectTimeout = 5 * time.Second

var ErrSecurityRejected = errors.New("scrape destination rejected by public-network policy")

// These conservative static blocks follow the IANA IPv4/IPv6 Special-Purpose
// Address Registries (iana.org/assignments/iana-ipv4-special-registry and
// iana.org/assignments/iana-ipv6-special-registry), RFC 6890/8190. IPv6 is
// further limited to 2000::/3 global-unicast allocation space; translation and
// transition mechanisms (RFC 6052/3964/4380) are not needed by this scraper.
var deniedIPv4Prefixes = mustPrefixes(
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
	"169.254.0.0/16", "168.63.129.16/32", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24",
	"192.31.196.0/24", "192.52.193.0/24", "192.88.99.0/24", "192.168.0.0/16",
	"192.175.48.0/24", "198.18.0.0/15", "198.51.100.0/24",
	"203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
)

var ipv6GlobalUnicast = netip.MustParsePrefix("2000::/3")
var deniedIPv6Prefixes = mustPrefixes(
	"2001::/23",                      // IETF protocol assignments, including Teredo.
	"2002::/16",                      // 6to4 embeds an IPv4 endpoint.
	"2001:db8::/32",                  // Documentation, explicitly retained if policy is widened.
	"3fff::/20",                      // Documentation (RFC 9637).
	"64:ff9b::/96", "64:ff9b:1::/48", // IPv4/IPv6 translation prefixes.
)

func mustPrefixes(values ...string) []netip.Prefix {
	prefixes := make([]netip.Prefix, 0, len(values))
	for _, value := range values {
		prefixes = append(prefixes, netip.MustParsePrefix(value))
	}
	return prefixes
}

func isPublicAddress(address netip.Addr) bool {
	if !address.IsValid() || address.Zone() != "" {
		return false
	}
	address = address.Unmap()
	if address.Is4() {
		for _, prefix := range deniedIPv4Prefixes {
			if prefix.Contains(address) {
				return false
			}
		}
		return address.IsGlobalUnicast()
	}
	if !ipv6GlobalUnicast.Contains(address) {
		return false
	}
	for _, prefix := range deniedIPv6Prefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return address.IsGlobalUnicast()
}

// validateURL enforces syntax/authority/port policy and checks IP literals.
// It neither resolves names nor mutates the input string or parsed query.
func validateURL(raw string) (*url.URL, error) {
	rejected := func() (*url.URL, error) {
		return nil, &ScrapeError{Kind: ScrapeErrorSecurityRejected, Stage: ScrapeStageValidation, Message: "URL rejected by scraper policy", Cause: ErrSecurityRejected}
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || !parsed.IsAbs() || parsed.Opaque != "" || parsed.User != nil {
		return rejected()
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" {
		return rejected()
	}
	host := parsed.Hostname()
	if strings.Contains(host, "%") || strings.HasSuffix(parsed.Host, ":") {
		return rejected()
	}
	port := parsed.Port()
	if port != "" {
		if parsed.Scheme == "http" && port != "80" || parsed.Scheme == "https" && port != "443" {
			return rejected()
		}
	}
	address, ipErr := netip.ParseAddr(host)
	if ipErr == nil {
		if !isPublicAddress(address) {
			return rejected()
		}
		return parsed, nil
	}
	if !validDNSName(host) || isMetadataHostname(host) {
		return rejected()
	}
	return parsed, nil
}

func validDNSName(host string) bool {
	name := strings.TrimSuffix(host, ".")
	if name == "" || len(name) > 253 {
		return false
	}
	for _, r := range name {
		if r > 127 || !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.') {
			return false
		}
	}
	if isLegacyIPv4Form(name) {
		return false
	}
	for _, label := range strings.Split(name, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}

// isLegacyIPv4Form rejects complete one-to-four-component numeric address
// grammars (decimal/octal-looking or 0x-prefixed hexadecimal), not ordinary
// DNS labels that merely contain the substring "0x".
func isLegacyIPv4Form(name string) bool {
	components := strings.Split(name, ".")
	if len(components) < 1 || len(components) > 4 {
		return false
	}
	for _, component := range components {
		if component == "" {
			return false
		}
		if allDigits(component) {
			continue
		}
		if len(component) < 3 || component[0] != '0' || component[1] != 'x' && component[1] != 'X' {
			return false
		}
		for _, digit := range component[2:] {
			if !(digit >= '0' && digit <= '9' || digit >= 'a' && digit <= 'f' || digit >= 'A' && digit <= 'F') {
				return false
			}
		}
	}
	return true
}

func allDigits(value string) bool {
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return value != ""
}

func isMetadataHostname(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	for _, metadata := range []string{
		"metadata", "metadata.google.internal", "metadata.google", "instance-data",
		"instance-data.ec2.internal", "metadata.tencentyun.com", "metadata.azure.com", "metadata.aws.internal",
	} {
		if host == metadata || strings.HasSuffix(host, "."+metadata) {
			return true
		}
	}
	return false
}

type addressResolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}

type contextDialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

// newSafeDialer makes one bounded resolution-plus-dial operation per call.
// Its resolver and dialer parameters are internal seams; production callers use
// the system resolver and net.Dialer without any private-address override.
func newSafeDialer(timeout time.Duration, resolver addressResolver, dialer contextDialer) func(context.Context, string, string) (net.Conn, error) {
	if timeout <= 0 {
		timeout = defaultConnectTimeout
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	if dialer == nil {
		dialer = &net.Dialer{}
	}
	return func(parent context.Context, network, address string) (net.Conn, error) {
		ctx, cancel := context.WithTimeout(parent, timeout)
		defer cancel()
		fail := func(kind ScrapeErrorKind, cause error) (net.Conn, error) {
			return nil, &ScrapeError{Kind: kind, Stage: ScrapeStageFetch, Message: "network connection failed", Cause: cause}
		}
		if err := ctx.Err(); err != nil {
			return fail(contextErrorKind(err), err)
		}
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return fail(ScrapeErrorSecurityRejected, ErrSecurityRejected)
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil || !validDialPort(port) || !validDialHost(host) || isMetadataHostname(host) {
			return fail(ScrapeErrorSecurityRejected, ErrSecurityRejected)
		}
		if literal, parseErr := netip.ParseAddr(host); parseErr == nil {
			if !isPublicAddress(literal) {
				return fail(ScrapeErrorSecurityRejected, ErrSecurityRejected)
			}
			conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(literal.Unmap().String(), port))
			if dialErr != nil {
				return fail(networkErrorKind(ctx, dialErr), dialErr)
			}
			return conn, nil
		}
		if !validDNSName(host) {
			return fail(ScrapeErrorSecurityRejected, ErrSecurityRejected)
		}
		addresses, lookupErr := resolver.LookupNetIP(ctx, "ip", host)
		if lookupErr != nil {
			return fail(networkErrorKind(ctx, lookupErr), lookupErr)
		}
		if len(addresses) == 0 {
			return fail(ScrapeErrorSecurityRejected, ErrSecurityRejected)
		}
		approved := make([]netip.Addr, 0, len(addresses))
		for _, candidate := range addresses {
			candidate = candidate.Unmap()
			if !isPublicAddress(candidate) {
				return fail(ScrapeErrorSecurityRejected, ErrSecurityRejected)
			}
			approved = append(approved, candidate)
		}
		literal := approved[0].String()
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(literal, port))
		if dialErr != nil {
			return fail(networkErrorKind(ctx, dialErr), dialErr)
		}
		return conn, nil
	}
}

func validDialPort(port string) bool {
	return port == "80" || port == "443"
}

func validDialHost(host string) bool {
	if strings.Contains(host, "%") {
		return false
	}
	if address, err := netip.ParseAddr(host); err == nil {
		return address.Zone() == ""
	}
	return !strings.ContainsAny(host, ":[]")
}

func contextErrorKind(err error) ScrapeErrorKind {
	if errors.Is(err, context.Canceled) {
		return ScrapeErrorCanceled
	}
	return ScrapeErrorTimeout
}

func networkErrorKind(ctx context.Context, err error) ScrapeErrorKind {
	if ctx.Err() != nil {
		return contextErrorKind(ctx.Err())
	}
	if errors.Is(err, context.Canceled) {
		return ScrapeErrorCanceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return ScrapeErrorTimeout
	}
	return ScrapeErrorNetwork
}
