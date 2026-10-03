// Package queue publishes versioned digest jobs to Redis Streams.
package queue

import (
	"errors"
	"fmt"
	"net/url"

	"github.com/google/uuid"
)

// SchemaVersion is the current Redis Stream message contract version.
const SchemaVersion = 1

// Message is the stable payload carried in a Redis Stream entry's data field.
type Message struct {
	SchemaVersion int       `json:"schema_version"`
	JobID         uuid.UUID `json:"job_id"`
	URL           string    `json:"url"`
}

// Validate ensures the message is supported and complete before publication.
func (message Message) Validate() error {
	if message.SchemaVersion != SchemaVersion {
		return fmt.Errorf("unsupported schema_version %d", message.SchemaVersion)
	}
	if message.JobID == uuid.Nil {
		return errors.New("job_id is required")
	}
	parsed, err := url.ParseRequestURI(message.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Hostname() == "" {
		return errors.New("url must be an absolute HTTP(S) URL")
	}
	return nil
}
