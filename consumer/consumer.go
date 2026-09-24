package consumer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ErrPermanent - ошибка, которую бессмысленно повторять: битый формат, чужая схема.
// Хендлер оборачивает ее (fmt.Errorf("parse: %w", consumer.ErrPermanent)),
// и запись уезжает в DLQ сразу, без ретраев и пауз
var ErrPermanent = errors.New("permanent error")

// Handler обрабатывает одну запись.
//
// Возврат ошибки приводит к повторным попыткам (Config.HandlerMaxAttempts),
// после которых запись уезжает в Config.DLQ, а если DLQ не настроена -
// останавливает [Consumer.Run]: батч не коммитится и будет перечитан
// заново (at-least-once).
//
// Хендлер должен быть идемпотентным: одна и та же запись может приехать повторно
type Handler func(ctx context.Context, r *kgo.Record) error

type Consumer struct {
	cfg     Config
	client  *kgo.Client
	handler Handler
}

func NewConsumer(cfg Config, handler Handler) (*Consumer, error) {
	if err := cfg.ValidateConsumer(); err != nil {
		return nil, fmt.Errorf("consumer: %w", err)
	}
	if handler == nil {
		return nil, errors.New("consumer: handler is required")
	}

	client, err := kgo.NewClient(cfg.ConsumerOpts()...)
	if err != nil {
		return nil, fmt.Errorf("consumer client: %w", err)
	}

	return &Consumer{
		cfg:     cfg,
		client:  client,
		handler: handler,
	}, nil
}

// Run блокируется и читает записи, пока не отменят ctx или не закроют клиент
func (c *Consumer) Run(ctx context.Context) error {
	for {
		err := c.pollOnce(ctx)
		if err == nil {
			continue
		}

		// Штатная остановка: незакоммиченный батч перечитается после рестарта
		if ctx.Err() != nil || errors.Is(err, kgo.ErrClientClosed) {
			slog.LogAttrs(ctx, slog.LevelInfo, "consumer: stopped", slog.Any("reason", err))
			return nil
		}

		return err
	}
}

// pollOnce - одна итерация: poll -> обработка -> коммит -> AllowRebalance.
//
// Вынесено в отдельную функцию ради defer: пока не вызван AllowRebalance,
// ребаланс не начнется (см. kgo.BlockRebalanceOnPoll), поэтому партиции
// не уедут к другому консьюмеру, пока мы обрабатываем и коммитим батч
func (c *Consumer) pollOnce(ctx context.Context) error {
	fetches := c.client.PollRecords(ctx, c.cfg.MaxPollRecords)

	// Poll регистрирует поллера и блокирует ребаланс, даже если вернул ошибку,
	// поэтому разрешаем ребаланс на любом выходе
	defer c.client.AllowRebalance()

	if fetches.IsClientClosed() {
		return kgo.ErrClientClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Ошибки фетча не фатальны: клиент сам переподключится и перечитает
	fetches.EachError(func(topic string, partition int32, err error) {
		slog.LogAttrs(ctx, slog.LevelError, "consumer: fetch failed",
			slog.String("topic", topic),
			slog.Int("partition", int(partition)),
			slog.Any("error", err),
		)
	})

	for iter := fetches.RecordIter(); !iter.Done(); {
		if err := c.handle(ctx, iter.Next()); err != nil {
			return err // батч не коммитим
		}
	}

	// Коммитим только после успешной обработки всего батча.
	//
	// Контекст отвязан от отмены: батч уже обработан, и если не дать себя
	// закоммитить на shutdown, эти записи приедут повторно после перезапуска.
	// От зависания страхует CommitTimeout
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.cfg.CommitTimeout)
	defer cancel()

	if err := c.client.CommitUncommittedOffsets(commitCtx); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	return nil
}

// handle обрабатывает запись: ретраи, а если они не помогли - DLQ
func (c *Consumer) handle(ctx context.Context, r *kgo.Record) error {
	err := c.retry(ctx, c.cfg.HandlerMaxAttempts, "handler", r, func() error {
		return c.handler(ctx, r)
	})
	if err == nil {
		return nil
	}

	// DLQ выключена или нас останавливают: запись не теряем,
	// батч не коммитится и перечитается после рестарта
	if c.cfg.DLQ == nil || ctx.Err() != nil {
		return fmt.Errorf("handle record (topic %s, partition %d, offset %d): %w",
			r.Topic, r.Partition, r.Offset, err)
	}

	dlqErr := c.retry(ctx, c.cfg.DLQMaxAttempts, "dlq send", r, func() error {
		return c.cfg.DLQ.Send(ctx, r, err)
	})
	if dlqErr != nil {
		return fmt.Errorf("send to dlq (topic %s, partition %d, offset %d): %w",
			r.Topic, r.Partition, r.Offset, errors.Join(dlqErr, err))
	}

	slog.LogAttrs(ctx, slog.LevelWarn, "consumer: record sent to dlq",
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
func (c *Consumer) retry(ctx context.Context, attempts int, what string, r *kgo.Record, op func() error) error {
	var err error

	for attempt := 1; attempt <= attempts; attempt++ {
		if err = op(); err == nil {
			return nil
		}

		if errors.Is(err, ErrPermanent) || attempt == attempts {
			break
		}

		slog.LogAttrs(ctx, slog.LevelWarn, "consumer: "+what+" failed, retrying",
			slog.String("topic", r.Topic),
			slog.Int("partition", int(r.Partition)),
			slog.Int64("offset", r.Offset),
			slog.Int("attempt", attempt),
			slog.Any("error", err),
		)

		// Отмена ctx во время ожидания - штатная остановка
		if waitErr := c.cfg.Backoff.Wait(ctx, attempt); waitErr != nil {
			return errors.Join(err, waitErr)
		}
	}

	return err
}

// Закрывает клиент и выходит из группы, чтоб партиции сразу разъехались
// по живым консьюмерам, не дожидаясь SessionTimeout.
//
// Контекст не нужен: обработанные батчи коммитит сам [Consumer.Run],
// коммитить на закрытии нечего. Вызывать после возврата из Run.
//
// Config.DLQ не закрывает - ею владеет тот, кто ее создал
func (c *Consumer) Close() {
	c.client.Close()
}
