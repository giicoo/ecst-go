package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// processor обрабатывает одну запись: ретраи с бэкоффом, а если они
// не помогли - DLQ.
//
// Логика одна и та же независимо от того, кто читает партиции:
// [Consumer] в одной горутине или пул воркеров, где на каждую партицию своя
type processor struct {
	cfg     Config
	handler Handler
	log     *slog.Logger
}

func newProcessor(cfg Config, handler Handler, log *slog.Logger) *processor {
	if log == nil {
		log = slog.Default()
	}

	return &processor{
		cfg:     cfg,
		handler: handler,
		log:     log,
	}
}

// process обрабатывает запись: ретраи, а если они не помогли - DLQ.
//
// Ошибка означает, что запись не пристроена: коммитить ее офсет нельзя,
// иначе запись потеряется. Батч перечитается заново (at-least-once)
func (p *processor) process(ctx context.Context, r *kgo.Record) error {
	err := p.retry(ctx, p.cfg.HandlerMaxAttempts, "handler", r, func() error {
		return p.handler(ctx, r)
	})
	if err == nil {
		return nil
	}

	// Нас останавливают: запись не теряем,
	// батч не коммитится и перечитается после рестарта
	if ctx.Err() != nil {
		return fmt.Errorf("handle record (topic %s, partition %d, offset %d): %w",
			r.Topic, r.Partition, r.Offset, err)
	}

	dlqErr := p.retry(ctx, p.cfg.DLQMaxAttempts, "dlq send", r, func() error {
		return p.cfg.DLQ.Send(ctx, r, err)
	})
	if dlqErr != nil {
		return fmt.Errorf("send to dlq (topic %s, partition %d, offset %d): %w",
			r.Topic, r.Partition, r.Offset, errors.Join(dlqErr, err))
	}

	p.log.LogAttrs(ctx, slog.LevelWarn, "consumer: record sent to dlq",
		slog.String("topic", r.Topic),
		slog.Int("partition", int(r.Partition)),
		slog.Int64("offset", r.Offset),
		slog.Any("cause", err),
	)

	// Запись пристроена, офсет закоммитится вместе с батчем
	return nil
}

// retry повторяет op с экспоненциальной задержкой.
// Задержка нужна, чтоб не выжечь все попытки за миллисекунды,
// пока лежит база или соседний сервис.
//
// Прерывается сразу на [ErrPermanent] и на отмене ctx
func (p *processor) retry(ctx context.Context, attempts int, what string, r *kgo.Record, op func() error) error {
	var err error

	for attempt := 1; attempt <= attempts; attempt++ {
		if err = op(); err == nil {
			return nil
		}

		if errors.Is(err, ErrPermanent) || attempt == attempts {
			break
		}

		p.log.LogAttrs(ctx, slog.LevelWarn, "consumer: "+what+" failed, retrying",
			slog.String("topic", r.Topic),
			slog.Int("partition", int(r.Partition)),
			slog.Int64("offset", r.Offset),
			slog.Int("attempt", attempt),
			slog.Any("error", err),
		)

		// Отмена ctx во время ожидания - штатная остановка
		if waitErr := p.cfg.Backoff.Wait(ctx, attempt); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}

	return err
}
