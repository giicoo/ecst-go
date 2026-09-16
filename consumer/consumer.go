package consumer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/giicoo/ecst-go/config"
	"github.com/giicoo/ecst-go/envelope"
	"github.com/twmb/franz-go/pkg/kgo"
)

// сколько ждать коммит уже обработанной пачки при остановке
const commitTimeout = 10 * time.Second

type HandlerFunc func(ctx context.Context, record *kgo.Record) error

type Consumer struct {
	client   *kgo.Client
	handlers map[string]HandlerFunc

	mu sync.RWMutex // защищает handlers

	running atomic.Bool
	runMu   sync.Mutex // защищает cancel
	cancel  context.CancelFunc
	done    chan struct{}

	closeOnce sync.Once
}

func New(cfg config.Config) (*Consumer, error) {
	if err := cfg.ValidateConsumer(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	client, err := kgo.NewClient(cfg.ConsumerOpts()...)
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

// Run читает и обрабатывает записи, пока не отменят ctx или не вызовут
// Shutdown/Close. Штатная остановка — не ошибка, возвращается nil.
func (c *Consumer) Run(ctx context.Context) error {
	if !c.running.CompareAndSwap(false, true) {
		return fmt.Errorf("consumer: already running")
	}

	// собственный cancel, чтобы Shutdown мог остановить цикл без внешнего ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.runMu.Lock()
	c.cancel = cancel
	c.runMu.Unlock()

	defer close(c.done)

	for {
		if ctx.Err() != nil {
			return nil
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if ctx.Err() != nil {
			return nil
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
			// коммитим даже при отмене: записи уже обработаны,
			// иначе после рестарта их обработают повторно
			commitCtx, cancelCommit := context.WithTimeout(context.WithoutCancel(ctx), commitTimeout)
			err := c.client.CommitRecords(commitCtx, toCommit...)
			cancelCommit()
			if err != nil {
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

// Shutdown — штатная остановка: останавливает Run, ждёт его выхода
// (текущая пачка дообрабатывается и коммитится) и закрывает клиента.
// Ограничивается переданным контекстом; клиент закрывается в любом случае.
func (c *Consumer) Shutdown(ctx context.Context) error {
	defer c.Close()

	c.runMu.Lock()
	cancel := c.cancel
	c.runMu.Unlock()

	if cancel == nil {
		return nil // Run не запускался — ждать нечего
	}
	cancel()

	select {
	case <-c.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("consumer: shutdown timeout: %w", ctx.Err())
	}
}

// Close закрывает клиента немедленно, не дожидаясь выхода из Run.
// Идемпотентен, поэтому годится для defer рядом с Shutdown.
func (c *Consumer) Close() {
	c.closeOnce.Do(c.client.Close)
}
