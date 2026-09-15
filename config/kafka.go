package config

import "time"

type KafkaConfig struct {
	Brokers  []string
	ClientID string
	DialTimeout time.Duration
}
