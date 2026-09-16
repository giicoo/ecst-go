package config

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Kafka    KafkaConfig
	Consumer ConsumerConfig
	Producer ProducerConfig

	// Топики, которые приводит к описанному виду migrator.Migrate
	Topics []TopicConfig
}

// ValidateProducer — проверки, обязательные для producer.New.
func (c Config) ValidateProducer() error {
	return errors.Join(c.Kafka.Validate(), c.Producer.Validate())
}

// ValidateConsumer — проверки, обязательные для consumer.New.
func (c Config) ValidateConsumer() error {
	return errors.Join(c.Kafka.Validate(), c.Consumer.Validate())
}

// ValidateTopics — проверки, обязательные для migrator.Migrate.
func (c Config) ValidateTopics() error {
	if len(c.Topics) == 0 {
		return fmt.Errorf("topics: at least one topic is required")
	}
	return errors.Join(c.Kafka.Validate(), validateTopics(c.Topics))
}

// ProducerOpts — полный набор опций kgo.Client для продюсера.
func (c Config) ProducerOpts() []kgo.Opt {
	return append(c.Kafka.KgoOpts(), c.Producer.KgoOpts()...)
}

// ConsumerOpts — полный набор опций kgo.Client для консьюмера.
func (c Config) ConsumerOpts() []kgo.Opt {
	return append(c.Kafka.KgoOpts(), c.Consumer.KgoOpts()...)
}
