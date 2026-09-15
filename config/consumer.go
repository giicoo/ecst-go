package config

import (
	"fmt"
	"slices"
)

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
	if slices.Contains(c.Topics, "") {
		return fmt.Errorf("consumer: topic name must not be empty")
	}
	if c.StartOffset != "earliest" && c.StartOffset != "latest" {
		return fmt.Errorf("consumer: invalid start_offset %q, must be \"earliest\" or \"latest\"", c.StartOffset)
	}
	if !c.AutocommitDisable {
		return fmt.Errorf("consumer: autocommit must be disabled — library commits explicitly after successful handling")
	}
	return nil
}
