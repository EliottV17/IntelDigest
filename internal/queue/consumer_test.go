package queue

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type testRedisError string

func (err testRedisError) Error() string { return string(err) }
func (testRedisError) RedisError()       {}

type fakeConsumerRedis struct {
	commands [][]interface{}
	contexts []context.Context
	results  []interface{}
	errors   []error
	readArgs []redis.XReadGroupArgs
	readCtxs []context.Context
	readData [][]redis.XStream
	readErrs []error
	closed   int
}

func (fake *fakeConsumerRedis) Do(ctx context.Context, args ...interface{}) *redis.Cmd {
	fake.commands = append(fake.commands, append([]interface{}(nil), args...))
	fake.contexts = append(fake.contexts, ctx)
	cmd := redis.NewCmd(ctx, args...)
	if len(fake.errors) > 0 {
		cmd.SetErr(fake.errors[0])
		fake.errors = fake.errors[1:]
	} else if len(fake.results) > 0 {
		cmd.SetVal(fake.results[0])
		fake.results = fake.results[1:]
	}
	return cmd
}

func (fake *fakeConsumerRedis) XReadGroup(ctx context.Context, args *redis.XReadGroupArgs) *redis.XStreamSliceCmd {
	copyArgs := *args
	copyArgs.Streams = append([]string(nil), args.Streams...)
	fake.readArgs = append(fake.readArgs, copyArgs)
	fake.readCtxs = append(fake.readCtxs, ctx)
	cmd := redis.NewXStreamSliceCmd(ctx)
	if len(fake.readErrs) > 0 {
		cmd.SetErr(fake.readErrs[0])
		fake.readErrs = fake.readErrs[1:]
	} else if len(fake.readData) > 0 {
		cmd.SetVal(fake.readData[0])
		fake.readData = fake.readData[1:]
	}
	return cmd
}

func (fake *fakeConsumerRedis) Close() error { fake.closed++; return nil }

func TestConsumerEnsureGroupUsesStartAndOnlyAcceptsBusyGroup(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "created"},
		{name: "already exists", err: testRedisError("BUSYGROUP Consumer Group name already exists")},
		{name: "other error containing token", err: errors.New("prefix BUSYGROUP suffix"), want: true},
		{name: "wrong busy group message", err: testRedisError("BUSYGROUP something else"), want: true},
		{name: "authentication failure", err: testRedisError("WRONGPASS invalid username-password pair"), want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeConsumerRedis{errors: []error{test.err}}
			consumer := newFakeConsumer(fake)
			created, err := consumer.EnsureGroup(context.Background())
			if (err != nil) != test.want {
				t.Fatalf("EnsureGroup error = %v, want error %v", err, test.want)
			}
			if test.want && !errors.Is(err, test.err) {
				t.Fatalf("error %v does not preserve %v", err, test.err)
			}
			if !test.want && created != (test.name == "created") {
				t.Fatalf("created = %v", created)
			}
			want := []interface{}{"XGROUP", "CREATE", "jobs:test", "workers", "0", "MKSTREAM"}
			if !reflect.DeepEqual(fake.commands[0], want) {
				t.Fatalf("command = %#v, want %#v", fake.commands[0], want)
			}
		})
	}
}

func TestConsumerReadCommandAndDelivery(t *testing.T) {
	fake := &fakeConsumerRedis{readData: [][]redis.XStream{{
		{Stream: "jobs:test", Messages: []redis.XMessage{{ID: "1712345678901-2", Values: map[string]interface{}{"data": "not-json", "other": int64(7)}}}},
	}}}
	consumer := newFakeConsumer(fake)
	delivery, ok, err := consumer.Read(context.Background())
	if err != nil || !ok {
		t.Fatalf("Read = (%v, %v, %v)", delivery, ok, err)
	}
	if delivery.ID != "1712345678901-2" || !reflect.DeepEqual(delivery.Fields, map[string]interface{}{"data": "not-json", "other": int64(7)}) {
		t.Fatalf("delivery = %#v", delivery)
	}
	if len(fake.readArgs) != 1 {
		t.Fatalf("XREADGROUP calls = %d", len(fake.readArgs))
	}
	args := fake.readArgs[0]
	if args.Group != "workers" || args.Consumer != "worker-a" || !reflect.DeepEqual(args.Streams, []string{"jobs:test", ">"}) || args.Count != 1 || args.Block != 1250*time.Millisecond || args.NoAck {
		t.Fatalf("XREADGROUP args = %+v", args)
	}
}

func TestConsumerReadNormalEmptyAndPreservesFailures(t *testing.T) {
	for _, test := range []struct {
		name    string
		err     error
		wantErr error
	}{
		{name: "block timeout", err: redis.Nil},
		{name: "empty response"},
		{name: "group missing", err: testRedisError("NOGROUP No such consumer group")},
		{name: "canceled", err: context.Canceled, wantErr: context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeConsumerRedis{readErrs: []error{test.err}}
			c := newFakeConsumer(fake)
			_, ok, err := c.Read(context.Background())
			if ok {
				t.Fatal("unexpected delivery")
			}
			if test.wantErr != nil && !errors.Is(err, test.wantErr) {
				t.Fatalf("error %v does not preserve %v", err, test.wantErr)
			}
			if test.name == "group missing" && !IsGroupMissing(err) {
				t.Fatalf("NOGROUP not distinguishable: %v", err)
			}
			if test.wantErr == nil && test.name != "group missing" && err != nil {
				t.Fatalf("normal empty read returned %v", err)
			}
		})
	}
}

func TestConsumerAckUsesEntryIDAndCloseOwnsClient(t *testing.T) {
	fake := &fakeConsumerRedis{}
	consumer := newFakeConsumer(fake)
	if err := consumer.Ack(context.Background(), Delivery{ID: "1712345678901-2"}); err != nil {
		t.Fatal(err)
	}
	want := []interface{}{"XACK", "jobs:test", "workers", "1712345678901-2"}
	if !reflect.DeepEqual(fake.commands[0], want) {
		t.Fatalf("command = %#v", fake.commands[0])
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := consumer.Close(); err != nil {
		t.Fatal(err)
	}
	if fake.closed != 1 {
		t.Fatalf("Close calls = %d", fake.closed)
	}
}

func TestConsumerOptionsNormalizeFractionalBlock(t *testing.T) {
	options := ConsumerOptions{RedisURL: "redis://localhost:6379/0", Stream: "jobs", Group: "group", Name: "consumer", Block: 1500 * time.Microsecond, OperationTimeout: time.Second, DialTimeout: time.Second, PoolSize: 1}
	parsed, err := validateConsumerOptions(options)
	if err != nil {
		t.Fatalf("fractional millisecond block rejected: %v", err)
	}
	effectiveBlock, err := normalizeBlock(options.Block)
	if err != nil || effectiveBlock != time.Millisecond {
		t.Fatalf("normalized BLOCK = %v, %v; want 1ms", effectiveBlock, err)
	}
	if parsed.ReadTimeout != time.Second+time.Millisecond {
		t.Fatalf("socket timeout = %v, want normalized 1ms plus operation timeout", parsed.ReadTimeout)
	}
}

func TestConsumerOptionsBoundDurationsAndPool(t *testing.T) {
	valid := ConsumerOptions{RedisURL: "redis://user:secret@localhost:6379/0", Stream: "jobs", Group: "group", Name: "consumer", Block: time.Millisecond, OperationTimeout: time.Second, DialTimeout: time.Second, PoolSize: 2}
	for _, mutate := range []func(*ConsumerOptions){
		func(o *ConsumerOptions) { o.Block = 0 },
		func(o *ConsumerOptions) { o.Block = 999 * time.Microsecond },
		func(o *ConsumerOptions) { o.Block = time.Duration(1<<63 - 1); o.OperationTimeout = time.Second },
		func(o *ConsumerOptions) { o.OperationTimeout = 0 },
		func(o *ConsumerOptions) { o.OperationTimeout = -time.Second },
		func(o *ConsumerOptions) { o.DialTimeout = 0 },
		func(o *ConsumerOptions) { o.PoolSize = 0 },
		func(o *ConsumerOptions) { o.PoolSize = int(^uint(0)>>1) - 1 },
		func(o *ConsumerOptions) { o.Stream = "" },
		func(o *ConsumerOptions) { o.Group = " " },
		func(o *ConsumerOptions) { o.Name = "" },
		func(o *ConsumerOptions) { o.RedisURL = "redis://user:secret@:bad/0" },
	} {
		o := valid
		mutate(&o)
		_, err := validateConsumerOptions(o)
		if err == nil {
			t.Fatalf("invalid options accepted: %#v", o)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("validation leaked credentials: %v", err)
		}
	}
	parsed, err := validateConsumerOptions(valid)
	if err != nil {
		t.Fatalf("valid options rejected: %v", err)
	}
	if parsed.MaxRetries != -1 || !parsed.ContextTimeoutEnabled || parsed.PoolSize != 4 || parsed.ReadTimeout != time.Second+time.Millisecond || parsed.DialTimeout != time.Second {
		t.Fatalf("Redis options = %+v; retries/context/pool/deadlines are not safely bounded", parsed)
	}
}

func TestConsumerReadCancellationIsBoundedByOperationDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fake := &fakeConsumerRedis{readErrs: []error{context.Canceled}}
	_, _, err := newFakeConsumer(fake).Read(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Read error = %v", err)
	}
	if _, ok := fake.readCtxs[0].Deadline(); !ok {
		t.Fatal("Read did not impose a finite socket-operation deadline")
	}
}

func TestConsumerReadUsesBlockPlusOperationDeadline(t *testing.T) {
	fake := &fakeConsumerRedis{}
	consumer := newFakeConsumer(fake)
	_, _, err := consumer.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	deadline, ok := fake.readCtxs[0].Deadline()
	if !ok {
		t.Fatal("read context has no socket deadline")
	}
	remaining := time.Until(deadline)
	if remaining > consumer.block+consumer.operationTimeout || remaining < consumer.block+consumer.operationTimeout-time.Millisecond {
		t.Fatalf("read deadline budget = %v, want block + operation timeout", remaining)
	}
}

func TestConsumerEnsureGroupAndAckRespectOperationDeadline(t *testing.T) {
	fake := &fakeConsumerRedis{}
	consumer := newFakeConsumer(fake)
	_, err := consumer.EnsureGroup(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.Ack(context.Background(), Delivery{ID: "entry"}); err != nil {
		t.Fatal(err)
	}
	for index, ctx := range fake.contexts {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatalf("command %d has no operation deadline", index)
		}
		if remaining := time.Until(deadline); remaining > time.Second || remaining <= 0 {
			t.Fatalf("command %d deadline budget = %v", index, remaining)
		}
	}
}

func newFakeConsumer(client consumerRedisClient) *Consumer {
	return &Consumer{client: client, stream: "jobs:test", group: "workers", name: "worker-a", block: 1250 * time.Millisecond, operationTimeout: time.Second, readTimeout: 2250 * time.Millisecond}
}
