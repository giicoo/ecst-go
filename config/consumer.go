package config

import (
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kgo"
)

// допустимые значения полей ConsumerConfig
var validStartOffset = []string{"earliest", "latest"}

type ConsumerConfig struct {
	GroupID           string
	Topics            []string
	StartOffset       string // "earliest" | "latest"
	AutocommitDisable bool
}

func DefaultConsumerConfig(groupID string, topics []string) ConsumerConfig {
	return ConsumerConfig{
		GroupID:           groupID,
		Topics:            topics,
		StartOffset:       "earliest",
		AutocommitDisable: true, // коммитим вручную после успешной обработки
	}
}

func (c ConsumerConfig) Validate() error {
	if c.GroupID == "" {
		return fmt.Errorf("consumer: group_id is required")
	}

	if len(c.Topics) == 0 {
		return fmt.Errorf("consumer: at least one topic is required")
	}
	for i, t := range c.Topics {
		if t == "" {
			return fmt.Errorf("consumer: topic name must not be empty")
		}
		if slices.Contains(c.Topics[:i], t) {
			return fmt.Errorf("consumer: duplicate topic %q", t)
		}
	}

	if !slices.Contains(validStartOffset, c.StartOffset) {
		return fmt.Errorf("consumer: invalid start_offset %q, must be one of %v", c.StartOffset, validStartOffset)
	}

	if !c.AutocommitDisable {
		return fmt.Errorf("consumer: autocommit must be disabled — library commits explicitly after successful handling")
	}

	return nil
}

// KgoOpts — consumer-часть опций клиента; общая часть в KafkaConfig.KgoOpts.
func (c ConsumerConfig) KgoOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.ConsumerGroup(c.GroupID),
		kgo.ConsumeTopics(c.Topics...),
		kgo.DisableAutoCommit(), // коммитим вручную в Consumer.Run
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	}

	// с какого места читать группу, у которой ещё нет закоммиченных офсетов
	switch c.StartOffset {
	case "latest":
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	default:
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	return opts
}
