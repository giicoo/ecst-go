package consumer

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/giicoo/ecst-go/backoff"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeDLQ считает отправки и умеет падать
type fakeDLQ struct {
	calls int
	err   error
}

func (f *fakeDLQ) Send(context.Context, *kgo.Record, error) error {
	f.calls++
	return f.err
}

// testConsumer собирает консьюмера без kgo-клиента: handle его не трогает
func testConsumer(t *testing.T, dlq DLQ, handler Handler) *Consumer {
	t.Helper()

	cfg := DefaultConfig("localhost:9092")
	cfg.Group = "g"
	cfg.Topics = []string{"t"}
	cfg.DLQ = dlq
	cfg.Backoff = backoff.Config{Min: time.Millisecond, Max: time.Millisecond, Factor: 1, Jitter: 0}

	if err := cfg.ValidateConsumer(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	return &Consumer{cfg: cfg, handler: handler}
}

func record() *kgo.Record {
	return &kgo.Record{Topic: "t", Partition: 1, Offset: 42, Value: []byte("v")}
}

func TestHandleRetriesThenDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	calls := 0

	c := testConsumer(t, dlq, func(context.Context, *kgo.Record) error {
		calls++
		return errors.New("boom")
	})

	if err := c.handle(context.Background(), record()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if calls != c.cfg.HandlerMaxAttempts {
		t.Fatalf("handler calls = %d, want %d", calls, c.cfg.HandlerMaxAttempts)
	}
	if dlq.calls != 1 {
		t.Fatalf("dlq calls = %d, want 1", dlq.calls)
	}
}

func TestHandlePermanentGoesStraightToDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	calls := 0

	c := testConsumer(t, dlq, func(context.Context, *kgo.Record) error {
		calls++
		return fmt.Errorf("parse: %w", ErrPermanent)
	})

	if err := c.handle(context.Background(), record()); err != nil {
		t.Fatalf("handle: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
	if dlq.calls != 1 {
		t.Fatalf("dlq calls = %d, want 1", dlq.calls)
	}
}

func TestHandleDLQFailureStopsConsumer(t *testing.T) {
	dlq := &fakeDLQ{err: errors.New("broker down")}
	cause := errors.New("boom")

	c := testConsumer(t, dlq, func(context.Context, *kgo.Record) error { return cause })

	err := c.handle(context.Background(), record())
	if err == nil {
		t.Fatal("handle: want error")
	}
	if dlq.calls != c.cfg.DLQMaxAttempts {
		t.Fatalf("dlq calls = %d, want %d", dlq.calls, c.cfg.DLQMaxAttempts)
	}
	// Причина исходного сбоя не теряется - она нужна, чтоб понять, что чинить
	if !errors.Is(err, cause) {
		t.Fatalf("error must wrap handler cause: %v", err)
	}
}

func TestHandleWithoutDLQStops(t *testing.T) {
	c := testConsumer(t, nil, func(context.Context, *kgo.Record) error { return errors.New("boom") })

	if err := c.handle(context.Background(), record()); err == nil {
		t.Fatal("handle: want error")
	}
}

func TestKafkaDLQValidation(t *testing.T) {
	if _, err := NewKafkaDLQ(nil, "dlq"); err == nil {
		t.Fatal("want error on nil producer")
	}
}
