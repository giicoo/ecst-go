package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/giicoo/ecst-go/config"
	"github.com/giicoo/ecst-go/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

type HandlerFunc func(ctx context.Context, record *kgo.Record) error

type Consumer struct {
	client   *kgo.Client
	handlers map[string]HandlerFunc

	mu   sync.RWMutex
	done chan struct{}
}

func New(cfg config.Config) (*Consumer, error) {
	if err := cfg.Consumer.Validate(); err != nil {
		return nil, fmt.Errorf("invalid consumer config: %w", err)
	}

	opts, err := toKgoOpts(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kgo opts: %w", err)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("new kgo client: %w", err)
	}

	return &Consumer{
		client:   client,
		handlers: make(map[string]HandlerFunc),
		done:     make(chan struct{}),
	}, nil
}

func (c *Consumer) Handle(entityType string, h HandlerFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.handlers[entityType] = h
}

func (c *Consumer) Run(ctx context.Context) error {
	defer close(c.done)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		var toCommit []*kgo.Record

		fetches.EachRecord(func(record *kgo.Record) {
			if err := c.dispatch(ctx, record); err != nil {
				// не коммитим — сообщение будет вычитано заново при следующем PollFetches/рестарте
				return
			}
			toCommit = append(toCommit, record)
		})

		fetches.EachError(func(topic string, partition int32, err error) {
			// TODO: observability/logger.go
		})

		if len(toCommit) > 0 {
			if err := c.client.CommitRecords(ctx, toCommit...); err != nil {
				return fmt.Errorf("commit records: %w", err)
			}
		}
	}
}

func (c *Consumer) dispatch(ctx context.Context, record *kgo.Record) error {
	entityType := headerValue(record, envelope.EnvelopeType)

	c.mu.RLock()
	h, ok := c.handlers[entityType]
	c.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no handler for entity_type=%q", entityType)
	}
	return h(ctx, record)
}

func headerValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *Consumer) Shutdown(ctx context.Context) error {
	select {
	case <-c.done:
	case <-ctx.Done():
		return fmt.Errorf("shutdown timeout: %w", ctx.Err())
	}
	c.client.Close()
	return nil
}
