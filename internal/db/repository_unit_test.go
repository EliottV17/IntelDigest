package db

import (
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type execCall struct {
	ctx  context.Context
	sql  string
	args []any
}

type repositoryDBTX struct {
	calls []execCall
	tag   pgconn.CommandTag
	err   error
}

func (f *repositoryDBTX) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("unexpected QueryRow call")
}

func (f *repositoryDBTX) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.calls = append(f.calls, execCall{ctx: ctx, sql: sql, args: args})
	return f.tag, f.err
}

func TestTryMarkProcessing(t *testing.T) {
	for _, tt := range []struct {
		name string
		rows string
		want bool
	}{
		{name: "claim acquired", rows: "UPDATE 1", want: true},
		{name: "claim not acquired", rows: "UPDATE 0", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			id := uuid.New()
			ctx := context.WithValue(context.Background(), struct{}{}, "claim-context")
			fake := &repositoryDBTX{tag: pgconn.NewCommandTag(tt.rows)}
			claimed, err := NewRepository(fake).TryMarkProcessing(ctx, id)
			if err != nil || claimed != tt.want {
				t.Fatalf("TryMarkProcessing() = (%v, %v), want (%v, nil)", claimed, err, tt.want)
			}
			if len(fake.calls) != 1 {
				t.Fatalf("Exec calls = %d, want 1", len(fake.calls))
			}
			call := fake.calls[0]
			for _, part := range []string{"UPDATE jobs", "status = 'processing'", "error = NULL", "updated_at = NOW()", "WHERE id = $1", "status = 'pending'"} {
				if !strings.Contains(call.sql, part) {
					t.Errorf("claim SQL missing %q", part)
				}
			}
			if len(call.args) != 1 || call.args[0] != id {
				t.Errorf("claim parameters = %#v, want UUID %v", call.args, id)
			}
			if call.ctx != ctx {
				t.Error("claim did not forward context")
			}
		})
	}
}

func TestTryMarkProcessingWrapsExecError(t *testing.T) {
	cause := errors.New("database unavailable")
	fake := &repositoryDBTX{err: cause}
	claimed, err := NewRepository(fake).TryMarkProcessing(context.Background(), uuid.New())
	if claimed || !errors.Is(err, cause) {
		t.Fatalf("TryMarkProcessing() = (%v, %v), want false and wrapped cause", claimed, err)
	}
}

func TestRecordScrapeError(t *testing.T) {
	for _, tt := range []struct {
		name string
		text string
		want string
	}{
		{name: "ASCII at limit", text: strings.Repeat("a", 1024), want: strings.Repeat("a", 1024)},
		{name: "ASCII over limit", text: strings.Repeat("b", 1025), want: strings.Repeat("b", 1024)},
		{name: "Unicode at limit", text: strings.Repeat("界", 1024), want: strings.Repeat("界", 1024)},
		{name: "Unicode over limit", text: strings.Repeat("🙂", 1025), want: strings.Repeat("🙂", 1024)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			id := uuid.New()
			ctx := context.WithValue(context.Background(), struct{}{}, "diagnostic-context")
			fake := &repositoryDBTX{tag: pgconn.NewCommandTag("UPDATE 1")}
			updated, err := NewRepository(fake).RecordScrapeError(ctx, id, tt.text)
			if err != nil || !updated {
				t.Fatalf("RecordScrapeError() = (%v, %v), want (true, nil)", updated, err)
			}
			call := fake.calls[0]
			for _, part := range []string{"UPDATE jobs", "error = $2", "updated_at = NOW()", "WHERE id = $1", "status = 'processing'"} {
				if !strings.Contains(call.sql, part) {
					t.Errorf("diagnostic SQL missing %q", part)
				}
			}
			if len(call.args) != 2 || call.args[0] != id || call.args[1] != tt.want {
				t.Errorf("diagnostic parameters = %#v, want UUID and capped string", call.args)
			}
			if got := utf8.RuneCountInString(call.args[1].(string)); got > 1024 {
				t.Errorf("diagnostic has %d characters, want at most 1024", got)
			}
			if call.ctx != ctx {
				t.Error("diagnostic did not forward context")
			}
		})
	}
}

func TestRecordScrapeErrorDoesNotUpdateNonProcessingJob(t *testing.T) {
	fake := &repositoryDBTX{tag: pgconn.NewCommandTag("UPDATE 0")}
	updated, err := NewRepository(fake).RecordScrapeError(context.Background(), uuid.New(), "safe reason")
	if err != nil || updated {
		t.Fatalf("RecordScrapeError() = (%v, %v), want (false, nil)", updated, err)
	}
}

func TestRepositoryUpdatesForwardCanceledContextAndWrapError(t *testing.T) {
	cause := errors.New("query canceled")
	for _, invoke := range []struct {
		name string
		call func(*Repository, context.Context, uuid.UUID) error
	}{
		{name: "claim", call: func(r *Repository, ctx context.Context, id uuid.UUID) error {
			_, err := r.TryMarkProcessing(ctx, id)
			return err
		}},
		{name: "diagnostic", call: func(r *Repository, ctx context.Context, id uuid.UUID) error {
			_, err := r.RecordScrapeError(ctx, id, "safe reason")
			return err
		}},
	} {
		t.Run(invoke.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			fake := &repositoryDBTX{err: cause}
			err := invoke.call(NewRepository(fake), ctx, uuid.New())
			if !errors.Is(err, cause) {
				t.Fatalf("error = %v, want wrapped DB cause", err)
			}
			if fake.calls[0].ctx != ctx || fake.calls[0].ctx.Err() != context.Canceled {
				t.Error("canceled context was not forwarded to DBTX")
			}
		})
	}
}
