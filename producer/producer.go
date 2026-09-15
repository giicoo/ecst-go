package producer

import (
	"context"
	"fmt"

	"github.com/giicoo/ecst-go/config"
	"github.com/giicoo/ecst-go/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
}

func New(cfg config.Config) (*Producer, error) {
	opts, err := toKgoOpts(cfg)
	if err != nil {
		return nil, fmt.Errorf("build kgo opts: %w", err)
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("new kgo client: %w", err)
	}
	
	return &Producer{client: client}, nil
}

func Publish[T any](ctx context.Context, p *Producer, topic string, ev envelope.Envelope[T]) error {
	var value []byte
	var err error

	if ev.Op == envelope.OpDelete {
		value = nil
	} else {
		value, err = ev.Encode()
		if err != nil {
			return fmt.Errorf("encode envelope: %w", err)
		}
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(ev.EntityID),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: envelope.EnvelopeType, Value: []byte(ev.EntityType)},
			{Key: envelope.TraceId, Value: []byte(ev.TraceID)},
		},
	}

	resultCh := make(chan error, 1)
	p.client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		resultCh <- err
	})

	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Producer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

func (p *Producer) Close() {
	p.client.Close()
}
