package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"inteldigest/internal/api"
	"inteldigest/internal/db"
	"inteldigest/internal/models"

	"github.com/google/uuid"
)

// fakeRepo is a manual test double for api.JobRepository.
type fakeRepo struct {
	jobs map[uuid.UUID]*models.Job
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{jobs: make(map[uuid.UUID]*models.Job)}
}

func (f *fakeRepo) CreateJob(ctx context.Context, url string) (*models.Job, error) {
	j := &models.Job{
		ID:        uuid.New(),
		URL:       url,
		Status:    models.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	f.jobs[j.ID] = j
	return j, nil
}

func (f *fakeRepo) GetJobByID(ctx context.Context, id uuid.UUID) (*models.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return nil, db.ErrNotFound
	}
	return j, nil
}

// --- POST /api/v1/digests ---

func TestPostDigest_ValidURL(t *testing.T) {
	repo := newFakeRepo()
	handler := api.NewRouter(repo)

	body := `{"url":"https://example.com/article"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/digests", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", w.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON response: %v", err)
	}
	if _, ok := resp["job_id"]; !ok {
		t.Error("response missing job_id")
	}
}

func TestPostDigest_InvalidURL(t *testing.T) {
	repo := newFakeRepo()
	handler := api.NewRouter(repo)

	cases := []struct {
		name string
		body string
	}{
		{"empty url", `{"url":""}`},
		{"private ip", `{"url":"http://127.0.0.1/x"}`},
		{"bad scheme", `{"url":"ftp://example.com"}`},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/digests", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", w.Code)
			}
		})
	}
}

func TestPostDigest_MalformedBody(t *testing.T) {
	repo := newFakeRepo()
	handler := api.NewRouter(repo)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/digests", bytes.NewBufferString("not json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// Ensure errors is used (for future tests).
var _ = errors.New

// --- GET /api/v1/digests/{id} ---

func TestGetDigest_Found(t *testing.T) {
	repo := newFakeRepo()
	handler := api.NewRouter(repo)

	// Create a job first via POST.
	body := `{"url":"https://example.com/test"}`
	createReq := httptest.NewRequest(http.MethodPost, "/api/v1/digests", bytes.NewBufferString(body))
	createReq.Header.Set("Content-Type", "application/json")
	createW := httptest.NewRecorder()
	handler.ServeHTTP(createW, createReq)

	var createResp map[string]any
	json.Unmarshal(createW.Body.Bytes(), &createResp)
	jobID := createResp["job_id"].(string)

	// GET the job.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/digests/"+jobID, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["id"] != jobID {
		t.Errorf("id = %v, want %v", resp["id"], jobID)
	}
	if resp["status"] != "pending" {
		t.Errorf("status = %v", resp["status"])
	}
}

func TestGetDigest_NotFound(t *testing.T) {
	repo := newFakeRepo()
	handler := api.NewRouter(repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/digests/"+uuid.New().String(), nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestGetDigest_BadID(t *testing.T) {
	repo := newFakeRepo()
	handler := api.NewRouter(repo)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/digests/not-a-uuid", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

// --- GET /healthz ---

func TestHealthz(t *testing.T) {
	repo := newFakeRepo()
	handler := api.NewRouter(repo)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("status = %v, want ok", resp["status"])
	}
}
