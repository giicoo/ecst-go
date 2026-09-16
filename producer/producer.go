package producer

import (
	"context"
	"fmt"
	"sync"

	"github.com/giicoo/ecst-go/config"
	"github.com/giicoo/ecst-go/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client    *kgo.Client
	closeOnce sync.Once
}

func New(cfg config.Config) (*Producer, error) {
	if err := cfg.ValidateProducer(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	client, err := kgo.NewClient(cfg.ProducerOpts()...)
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

// Flush ждёт подтверждения брокером всех записей из буфера.
// Возвращает ctx.Err(), если ожидание отменили.
func (p *Producer) Flush(ctx context.Context) error {
	return p.client.Flush(ctx)
}

// Shutdown — штатная остановка: дожидается доставки буфера и закрывает клиента.
// Ограничивается переданным контекстом; клиент закрывается в любом случае.
func (p *Producer) Shutdown(ctx context.Context) error {
	defer p.Close()

	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("producer: flush: %w", err)
	}
	return nil
}

// Close закрывает клиента немедленно, недоставленные записи теряются.
// Идемпотентен, поэтому годится для defer рядом с Shutdown.
func (p *Producer) Close() {
	p.closeOnce.Do(p.client.Close)
}
