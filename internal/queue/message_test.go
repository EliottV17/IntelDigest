package queue

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestMessageJSONContract(t *testing.T) {
	jobID := uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
	message := Message{SchemaVersion: 1, JobID: jobID, URL: "https://example.com/article"}

	encoded, err := json.Marshal(message)
	if err != nil {
		t.Fatalf("marshal message: %v", err)
	}
	const want = `{"schema_version":1,"job_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8","url":"https://example.com/article"}`
	if string(encoded) != want {
		t.Fatalf("message JSON = %s, want %s", encoded, want)
	}
}

func TestMessageValidateRejectsUnsupportedOrIncompleteMessage(t *testing.T) {
	valid := Message{SchemaVersion: 1, JobID: uuid.New(), URL: "https://example.com/article"}
	tests := []struct {
		name    string
		message Message
	}{
		{name: "unsupported version", message: Message{SchemaVersion: 2, JobID: valid.JobID, URL: valid.URL}},
		{name: "missing job id", message: Message{SchemaVersion: 1, URL: valid.URL}},
		{name: "missing URL", message: Message{SchemaVersion: 1, JobID: valid.JobID}},
		{name: "invalid URL", message: Message{SchemaVersion: 1, JobID: valid.JobID, URL: "not a URL"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.message.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestNewPublisherRejectsInvalidConfiguration(t *testing.T) {
	_, err := NewPublisher(nil, "http://localhost:6379", "jobs", 0, 0)
	if err == nil {
		t.Fatal("expected invalid Redis URL to fail")
	}
	if !strings.Contains(err.Error(), "redis") {
		t.Fatalf("error should identify Redis configuration, got %q", err)
	}
}
