package models_test

import (
	"testing"
	"time"

	"inteldigest/internal/models"

	"github.com/google/uuid"
)

func TestJobFieldsExist(t *testing.T) {
	now := time.Now()
	j := models.Job{
		ID:        uuid.New(),
		URL:       "https://example.com",
		Status:    models.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if j.ID == uuid.Nil {
		t.Fatal("expected non-nil UUID")
	}
	if j.URL != "https://example.com" {
		t.Fatalf("expected URL, got %s", j.URL)
	}
	if j.Result != nil {
		t.Fatal("expected nil Result")
	}
	if j.Error != nil {
		t.Fatal("expected nil Error")
	}
}

func TestStatusConstants(t *testing.T) {
	tests := []struct {
		status models.JobStatus
		want   string
	}{
		{models.StatusPending, "pending"},
		{models.StatusProcessing, "processing"},
		{models.StatusCompleted, "completed"},
		{models.StatusFailed, "failed"},
	}
	for _, tt := range tests {
		if string(tt.status) != tt.want {
			t.Errorf("expected %q, got %q", tt.want, tt.status)
		}
	}
}

func TestValidateURL(t *testing.T) {
	valid := []string{
		"https://example.com/article",
		"http://blog.dev/post/1",
		"https://sub.domain.io/path?q=1",
	}
	for _, u := range valid {
		if err := models.ValidateURL(u); err != nil {
			t.Errorf("ValidateURL(%q) unexpected error: %v", u, err)
		}
	}

	invalid := []struct {
		url    string
		substr string // substring expected in error message
	}{
		{"", "empty"},
		{"ftp://example.com", "scheme"},
		{"not-a-url", "scheme"},
		{"https://", "host"},
		{"http://127.0.0.1/path", "private"},
		{"http://localhost/path", "private"},
		{"http://10.0.0.1/x", "private"},
		{"http://192.168.1.1/x", "private"},
		{"http://169.254.169.254/latest", "private"},
	}
	for _, tt := range invalid {
		err := models.ValidateURL(tt.url)
		if err == nil {
			t.Errorf("ValidateURL(%q) expected error containing %q, got nil", tt.url, tt.substr)
			continue
		}
		if !containsIgnoreCase(err.Error(), tt.substr) {
			t.Errorf("ValidateURL(%q) error = %q, expected substring %q", tt.url, err.Error(), tt.substr)
		}
	}
}

func containsIgnoreCase(s, substr string) bool {
	return len(s) >= len(substr) && containsCI(s, substr)
}

func containsCI(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if eqFold(s[i:i+len(sub)], sub) {
			return true
		}
	}
	return false
}

func eqFold(a, b string) bool {
	for i := range a {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 32
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}
