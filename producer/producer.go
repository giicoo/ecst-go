package producer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kerr"
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

// Result - что стало с одной записью в [Producer.ProduceSyncResults]
type Result struct {
	Record *kgo.Record

	// nil, если запись сохранена брокером
	Err error
}

// ProduceSyncResults блокируется до подтверждения всех записей и возвращает
// результат по каждой.
//
// В отличие от [Producer.ProduceSync] не сводит батч к первой ошибке:
// одна незаписанная запись не делает неизвестной судьбу остальных.
// Нужен там, где по каждой записи свое решение - например в outbox-таблице,
// где отмечать отправленными надо только доехавшие строки.
//
// Порядок результатов не совпадает с порядком аргументов: записи
// подтверждаются вразнобой, поэтому свою запись ищут по Result.Record
func (p *Producer) ProduceSyncResults(ctx context.Context, records ...*kgo.Record) []Result {
	produced := p.client.ProduceSync(ctx, records...)

	results := make([]Result, 0, len(produced))
	for _, r := range produced {
		results = append(results, Result{Record: r.Record, Err: r.Err})
	}

	return results
}

// Permanent сообщает, что запись не доедет и на повторе: брокер отверг ее
// саму, а не отказался ее сейчас принять.
//
// Такое дает MESSAGE_TOO_LARGE, INVALID_RECORD, отказ авторизации.
// Таймауты, недоступный брокер и неизвестный пока топик - не сюда:
// они лечатся повтором
func Permanent(err error) bool {
	var kerror *kerr.Error

	return errors.As(err, &kerror) && !kerr.IsRetriable(err)
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
