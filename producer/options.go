package producer

import (
	"time"

	"github.com/giicoo/ecst-go/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

func toKgoOpts(cfg config.Config) ([]kgo.Opt, error) {
	pc := cfg.Producer

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.Brokers...),

		kgo.RetryBackoffFn(exponentialBackoff(pc.RetryBackoffBase, pc.RetryBackoffMax)),
		kgo.RecordRetries(int(pc.Retries)),
		kgo.RecordDeliveryTimeout(pc.RecordTimeout),
	}

	switch pc.Partitioner {
	case "round-robin":
		opts = append(opts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	default:
		opts = append(opts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	}

	for _, c := range compressionCodecs(pc.Compression) {
		opts = append(opts, kgo.ProducerBatchCompression(c))
	}

	switch pc.Acks {
	case "all":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "leader":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case "none":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	}

	if pc.TransactionalID != "" {
		opts = append(opts, kgo.TransactionalID(pc.TransactionalID))
		if pc.TxnTimeout > 0 {
			opts = append(opts, kgo.TransactionTimeout(pc.TxnTimeout))
		}
	} else if !pc.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	return opts, nil
}

func exponentialBackoff(base, max time.Duration) func(int) time.Duration {
	return func(tries int) time.Duration {
		d := base * time.Duration(1<<tries)
		if d > max {
			return max
		}
		return d
	}
}

func compressionCodecs(names []string) []kgo.CompressionCodec {
	var codecs []kgo.CompressionCodec
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

