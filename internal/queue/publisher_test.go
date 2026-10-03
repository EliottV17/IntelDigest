package queue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

type fakeRedisClient struct {
	stream string
	values interface{}
	err    error
}

func (client *fakeRedisClient) Ping(ctx context.Context) *redis.StatusCmd {
	command := redis.NewStatusCmd(ctx)
	command.SetVal("PONG")
	return command
}

func (client *fakeRedisClient) XAdd(ctx context.Context, args *redis.XAddArgs) *redis.StringCmd {
	client.stream = args.Stream
	client.values = args.Values
	command := redis.NewStringCmd(ctx)
	command.SetErr(client.err)
	return command
}

func (client *fakeRedisClient) Close() error { return nil }

func TestPublishWritesContractToConfiguredStream(t *testing.T) {
	client := &fakeRedisClient{}
	publisher := &Publisher{client: client, stream: "jobs:test", publishTimeout: time.Second}
	message := Message{SchemaVersion: SchemaVersion, JobID: uuid.New(), URL: "https://example.com/article"}

	if err := publisher.Publish(context.Background(), message); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if client.stream != "jobs:test" {
		t.Errorf("stream = %q, want jobs:test", client.stream)
	}
	values, ok := client.values.(map[string]interface{})
	if !ok {
		t.Fatalf("XADD values have type %T, want map[string]interface{}", client.values)
	}
	data, ok := values["data"].(string)
	if !ok {
		t.Fatalf("data field has type %T, want string", values["data"])
	}
	want := `{"schema_version":1,"job_id":"` + message.JobID.String() + `","url":"https://example.com/article"}`
	if data != want {
		t.Errorf("data = %s, want %s", data, want)
	}
}

func TestPublishWrapsRedisErrorWithStream(t *testing.T) {
	cause := errors.New("redis unavailable")
	publisher := &Publisher{
		client:         &fakeRedisClient{err: cause},
		stream:         "jobs:test",
		publishTimeout: time.Second,
	}
	message := Message{SchemaVersion: SchemaVersion, JobID: uuid.New(), URL: "https://example.com/article"}

	err := publisher.Publish(context.Background(), message)
	if !errors.Is(err, cause) {
		t.Fatalf("Publish error = %v, want wrapped cause", err)
	}
	if err == nil || !strings.Contains(err.Error(), `stream "jobs:test"`) {
		t.Fatalf("Publish error should name stream, got %v", err)
	}
}
