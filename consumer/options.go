package consumer

import (
	"github.com/giicoo/ecst-go/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

func toKgoOpts(cfg config.Config) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Kafka.Brokers...),
		kgo.ConsumerGroup(cfg.Consumer.GroupID),
		kgo.ConsumeTopics(cfg.Consumer.Topics...),
		kgo.DisableAutoCommit(), // коммитим вручную в Run()
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}

	return opts, nil
}
