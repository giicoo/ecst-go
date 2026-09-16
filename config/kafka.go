package config

import (
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaConfig struct {
	// Название клиента - виден в логах и в каждом запросе
	ClientID string

	// Bootstrap-список
	Brokers []string

	// Время на установку соединения с брокером
	DialTimeout time.Duration

	// Точки наблюдения (сюда добавляется Prometheus)
	Hooks []kgo.Hook
}

func DefaultKafkaConfig(clientID string, brokers ...string) KafkaConfig {
	return KafkaConfig{
		ClientID:    clientID,
		Brokers:     brokers,
		DialTimeout: 10 * time.Second,
	}
}

func (c KafkaConfig) Validate() error {
	if c.ClientID == "" {
		return fmt.Errorf("kafka: client_id is required")
	}
	if len(c.Brokers) == 0 {
		return fmt.Errorf("kafka: brokers must not be empty")
	}
	for _, b := range c.Brokers {
		if err := validateBroker(b); err != nil {
			return err
		}
	}
	if c.DialTimeout < 0 {
		return fmt.Errorf("kafka: dial_timeout must be >= 0")
	}
	return nil
}

func validateBroker(broker string) error {
	if broker == "" {
		return fmt.Errorf("kafka: broker address must not be empty")
	}
	host, port, err := net.SplitHostPort(broker)
	if err != nil {
		return fmt.Errorf("kafka: invalid broker %q, must be \"host:port\": %w", broker, err)
	}
	if host == "" {
		return fmt.Errorf("kafka: invalid broker %q, host must not be empty", broker)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p < 1 || p > 65535 {
		return fmt.Errorf("kafka: invalid broker %q, port must be in 1..65535", broker)
	}
	return nil
}

// KgoOpts — общие для producer и consumer опции подключения.
func (c KafkaConfig) KgoOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
	}

	if c.ClientID != "" {
		opts = append(opts, kgo.ClientID(c.ClientID))
	}

	if c.DialTimeout > 0 {
		opts = append(opts, kgo.DialTimeout(c.DialTimeout))
	}

	if len(c.Hooks) > 0 {
		opts = append(opts, kgo.WithHooks(c.Hooks...))
	}

	return opts
}
