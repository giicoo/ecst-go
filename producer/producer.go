package producer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer struct {
	client *kgo.Client
}

func NewProducer(cfg Config) (*Producer, error) {
	if err := cfg.ValidateProducer(); err != nil {
		return nil, fmt.Errorf("producer: %w", err)
	}

	client, err := kgo.NewClient(cfg.ProducerOpts()...)
	if err != nil {
		return nil, fmt.Errorf("producer client: %w", err)
	}

	return &Producer{
		client: client,
	}, nil
}

func (p *Producer) Produce(parentCtx context.Context, record *kgo.Record) {
	// Убираем возможность отмены, чтоб не обрубать отправку записи при shutdown
	// То, что мы не зависнем гарантирует RecordDeliveryTimeout, который обязательно выставлять
	ctx := context.WithoutCancel(parentCtx)

	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			slog.LogAttrs(ctx, slog.LevelError, "producer: record delivery failed",
				slog.String("topic", r.Topic),
				slog.String("key", string(r.Key)),
				slog.Any("error", err),
			)
			return
		}

		slog.LogAttrs(ctx, slog.LevelDebug, "producer: record delivered",
			slog.String("topic", r.Topic),
			slog.Int("partition", int(r.Partition)),
			slog.Int64("offset", r.Offset),
		)
	})
}

// Блокируется до подтверждения записи брокером.
// Нужен там, где нельзя двигаться дальше, пока запись не сохранена (например DLQ).
//
// В отличие от [Producer.Produce] контекст не отвязывается от отмены:
// вызывающий должен узнать, что на shutdown отправка не доехала
func (p *Producer) ProduceSync(ctx context.Context, records ...*kgo.Record) error {
	if err := p.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("produce sync: %w", err)
	}

	return nil
}

// Блокируется пока не обработаются все записи из буффера
// или не отменится контекст
func (p *Producer) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	return nil
}

// Вызывает [Flush] чтоб гарантировать обработку всех событий в памяти
func (p *Producer) Close(ctx context.Context) error {
	defer p.client.Close()

	if err := p.Flush(ctx); err != nil {
		return fmt.Errorf("close: %w", err)
	}

	return nil
}
