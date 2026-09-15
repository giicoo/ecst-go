package config

import "fmt"

type Config struct {
	Kafka          KafkaConfig
	Consumer       ConsumerConfig
	Producer ProducerConfig
}

func (c *Config) Validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka: brokers must not be empty")
	}

	if c.Consumer.GroupID != "" {
		if len(c.Consumer.Topics) == 0 {
			return fmt.Errorf("consumer: topics must not be empty when group_id is set")
		}
		if c.Consumer.StartOffset != "" &&
			c.Consumer.StartOffset != "earliest" &&
			c.Consumer.StartOffset != "latest" {
			return fmt.Errorf("consumer: invalid start_offset %q", c.Consumer.StartOffset)
		}
	}

	

	return c.Producer.Validate()
}
