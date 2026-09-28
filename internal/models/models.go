// Package models defines shared domain types for IntelDigest.
package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

// JobStatus represents the processing state of a job.
type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

// Job is the central work unit: a URL submitted for digest processing.
type Job struct {
	ID        uuid.UUID        `json:"id"`
	URL       string           `json:"url"`
	Status    JobStatus        `json:"status"`
	Result    *json.RawMessage `json:"result,omitempty"`
	Error     *string          `json:"error,omitempty"`
	CreatedAt time.Time        `json:"created_at"`
	UpdatedAt time.Time        `json:"updated_at"`
}

// ValidateURL checks that rawURL is a valid, non-private HTTP(S) URL.
func ValidateURL(rawURL string) error {
	if strings.TrimSpace(rawURL) == "" {
		return errors.New("url: empty")
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("url: invalid: %w", err)
	}

	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("url: scheme must be http or https, got %q", u.Scheme)
	}

	host := u.Hostname()
	if host == "" {
		return errors.New("url: host is empty")
	}

	if isPrivateHost(host) {
		return fmt.Errorf("url: private/reserved address %q", host)
	}

	return nil
}

// isPrivateHost returns true for loopback, RFC-1918, link-local, and
// cloud metadata addresses.
func isPrivateHost(host string) bool {
	if host == "localhost" {
		return true
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}

	return ip.IsLoopback() ||
		ip.IsPrivate() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast()
}
