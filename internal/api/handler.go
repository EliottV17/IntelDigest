// Package api defines the HTTP handlers and routing for IntelDigest.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"inteldigest/internal/db"
	"inteldigest/internal/models"
	"inteldigest/internal/queue"

	"github.com/google/uuid"
)

// JobRepository is the interface the handlers need from the persistence layer.
// Defined here (the consumer) so the dependency points inward.
type JobRepository interface {
	CreateJob(ctx context.Context, url string) (*models.Job, error)
	GetJobByID(ctx context.Context, id uuid.UUID) (*models.Job, error)
}

// JobPublisher publishes created jobs to the queue.
type JobPublisher interface {
	Publish(ctx context.Context, message queue.Message) error
}

// Handler holds dependencies for the HTTP handlers.
type Handler struct {
	repo      JobRepository
	publisher JobPublisher
}

// NewRouter creates an http.Handler with all routes registered.
func NewRouter(repo JobRepository, publisher JobPublisher) http.Handler {
	h := &Handler{repo: repo, publisher: publisher}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/digests", h.createDigest)
	mux.HandleFunc("GET /api/v1/digests/{id}", h.getDigest)
	mux.HandleFunc("GET /healthz", h.healthz)

	return mux
}

// createDigest handles POST /api/v1/digests.
func (h *Handler) createDigest(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if err := models.ValidateURL(req.URL); err != nil {
		respondError(w, http.StatusBadRequest, err.Error())
		return
	}

	job, err := h.repo.CreateJob(r.Context(), req.URL)
	if err != nil {
		slog.Error("creating job", "error", err)
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}

	message := queue.Message{
		SchemaVersion: queue.SchemaVersion,
		JobID:         job.ID,
		URL:           job.URL,
	}
	if err := h.publisher.Publish(r.Context(), message); err != nil {
		slog.Error("publishing job", "job_id", job.ID, "error", err)
		respondJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error":  "queue unavailable",
			"job_id": job.ID.String(),
		})
		return
	}

	respondJSON(w, http.StatusAccepted, map[string]any{
		"job_id":  job.ID,
		"message": "processing",
	})
}

// healthz handles GET /healthz.
func (h *Handler) healthz(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// getDigest handles GET /api/v1/digests/{id}.
func (h *Handler) getDigest(w http.ResponseWriter, r *http.Request) {
	rawID := r.PathValue("id")
	id, err := uuid.Parse(rawID)
	if err != nil {
		respondError(w, http.StatusBadRequest, "invalid job ID")
		return
	}

	job, err := h.repo.GetJobByID(r.Context(), id)
	if errors.Is(err, db.ErrNotFound) {
		respondError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		slog.Error("getting job", "error", err)
		respondError(w, http.StatusInternalServerError, "internal error")
		return
	}

	respondJSON(w, http.StatusOK, job)
}

// --- JSON helpers ---

func respondJSON(w http.ResponseWriter, code int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, code int, msg string) {
	respondJSON(w, code, map[string]string{"error": msg})
}
