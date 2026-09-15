package config

import (
	"fmt"
	"time"
)

type ProducerConfig struct {
	Idempotent bool   // защита от дублей при внутренних ретраях клиента

	TransactionalID string

	Compression   []string // порядок предпочтения: "zstd", "snappy", "gzip", "lz4"; fallback, если брокер не поддерживает первый

	Acks       string        // "all" (по умолчанию) | "leader" | "none"
	TxnTimeout time.Duration // таймаут транзакции; учитывается только при заданном TransactionalID

	Retries          int64
	RetryBackoffBase time.Duration
	RetryBackoffMax  time.Duration
	RecordTimeout    time.Duration // сколько ждать доставки одной записи, прежде чем окончательно считать её failed

	Partitioner string // "sticky-key" (по умолчанию, ключ=entity_id) | "round-robin"
}
func DefaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		Idempotent:       true,
		Compression:      []string{"zstd", "snappy"},
		Acks:             "all",
		Retries:          5,
		RetryBackoffBase: 200 * time.Millisecond,
		RetryBackoffMax:  5 * time.Second,
		RecordTimeout:    30 * time.Second,
		Partitioner:      "sticky-key",
	}
}

func (c ProducerConfig) Validate() error {
	if c.TransactionalID != "" && !c.Idempotent {
		return fmt.Errorf("producer: transactional_id requires idempotent=true")
	}
	if len(c.Compression) == 0 {
		return fmt.Errorf("producer: compression list must not be empty (use [\"none\"] explicitly if undesired)")
	}
	if c.Retries < 0 {
		return fmt.Errorf("producer: retries must be >= 0")
	}
	switch c.Acks {
	case "all", "leader", "none":
	default:
		return fmt.Errorf("producer: invalid acks %q, must be \"all\", \"leader\" or \"none\"", c.Acks)
	}
	if c.Acks != "all" && c.Idempotent {
		return fmt.Errorf("producer: idempotent writes require acks=\"all\"")
	}
	if c.TxnTimeout < 0 {
		return fmt.Errorf("producer: txn_timeout must be >= 0")
	}
	return nil
}
