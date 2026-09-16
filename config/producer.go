package config

import (
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// допустимые значения полей ProducerConfig
var (
	validCompression = []string{"zstd", "snappy", "gzip", "lz4", "none"}
	validAcks        = []string{"all", "leader", "none"}
	validPartitioner = []string{"sticky-key", "round-robin"}
)

type ProducerConfig struct {
	Idempotent bool // защита от дублей при внутренних ретраях клиента

	TransactionalID string

	Compression []string // порядок предпочтения: "zstd", "snappy", "gzip", "lz4"; fallback, если брокер не поддерживает первый

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
	for i, name := range c.Compression {
		if !slices.Contains(validCompression, name) {
			return fmt.Errorf("producer: invalid compression %q, must be one of %v", name, validCompression)
		}
		if slices.Contains(c.Compression[:i], name) {
			return fmt.Errorf("producer: duplicate compression %q", name)
		}
	}

	if !slices.Contains(validAcks, c.Acks) {
		return fmt.Errorf("producer: invalid acks %q, must be one of %v", c.Acks, validAcks)
	}
	if c.Acks != "all" && c.Idempotent {
		return fmt.Errorf("producer: idempotent writes require acks=\"all\"")
	}

	if !slices.Contains(validPartitioner, c.Partitioner) {
		return fmt.Errorf("producer: invalid partitioner %q, must be one of %v", c.Partitioner, validPartitioner)
	}

	if c.TxnTimeout < 0 {
		return fmt.Errorf("producer: txn_timeout must be >= 0")
	}
	if c.TxnTimeout > 0 && c.TransactionalID == "" {
		return fmt.Errorf("producer: txn_timeout is meaningless without transactional_id")
	}

	if c.Retries < 0 {
		return fmt.Errorf("producer: retries must be >= 0")
	}
	if c.RetryBackoffBase <= 0 {
		return fmt.Errorf("producer: retry_backoff_base must be > 0")
	}
	if c.RetryBackoffMax < c.RetryBackoffBase {
		return fmt.Errorf("producer: retry_backoff_max must be >= retry_backoff_base")
	}
	if c.RecordTimeout < 0 {
		return fmt.Errorf("producer: record_timeout must be >= 0")
	}

	return nil
}

// KgoOpts — producer-часть опций клиента; общая часть в KafkaConfig.KgoOpts.
func (c ProducerConfig) KgoOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.RetryBackoffFn(exponentialBackoff(c.RetryBackoffBase, c.RetryBackoffMax)),
		kgo.RecordRetries(int(c.Retries)),
		kgo.RecordDeliveryTimeout(c.RecordTimeout),
	}

	switch c.Partitioner {
	case "round-robin":
		opts = append(opts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	default:
		opts = append(opts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	}

	for _, codec := range compressionCodecs(c.Compression) {
		opts = append(opts, kgo.ProducerBatchCompression(codec))
	}

	switch c.Acks {
	case "leader":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case "none":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	default:
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}

	if c.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(c.TransactionalID))
		if c.TxnTimeout > 0 {
			opts = append(opts, kgo.TransactionTimeout(c.TxnTimeout))
		}
	} else if !c.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	return opts
}

func exponentialBackoff(base, max time.Duration) func(int) time.Duration {
	return func(tries int) time.Duration {
		if tries < 1 {
			tries = 1
		}
		// сдвиг ограничен, иначе 1<<tries переполняется и backoff уходит в минус
		if tries > 32 {
			return max
		}
		d := base * time.Duration(1<<(tries-1))
		if d <= 0 || d > max {
			return max
		}
		return d
	}
}

func compressionCodecs(names []string) []kgo.CompressionCodec {
	codecs := make([]kgo.CompressionCodec, 0, len(names))
	for _, n := range names {
		switch n {
		case "zstd":
			codecs = append(codecs, kgo.ZstdCompression())
		case "snappy":
			codecs = append(codecs, kgo.SnappyCompression())
		case "gzip":
			codecs = append(codecs, kgo.GzipCompression())
		case "lz4":
			codecs = append(codecs, kgo.Lz4Compression())
		case "none":
			codecs = append(codecs, kgo.NoCompression())
		}
	}
	return codecs
}
