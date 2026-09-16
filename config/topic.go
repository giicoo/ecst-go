package config

import "fmt"

// TopicConfig — описание топика для создания. Мигратор создаёт топик по нему,
// а существующий оставляет как есть.
type TopicConfig struct {
	Name              string
	Partitions        int32
	ReplicationFactor int16

	// Конфиги создаваемого топика: "cleanup.policy", "retention.ms",
	// "min.insync.replicas" и т.д.
	Configs map[string]string
}

// DefaultTopicConfig — топик под ECST: снапшот сущности лежит по ключу
// entity_id, удаление приезжает tombstone'ом, поэтому compaction, а не retention.
func DefaultTopicConfig(name string) TopicConfig {
	return TopicConfig{
		Name:              name,
		Partitions:        1,
		ReplicationFactor: 1,
		Configs:           map[string]string{"cleanup.policy": "compact"},
	}
}

func (c TopicConfig) Validate() error {
	if c.Name == "" {
		return fmt.Errorf("topics: name must not be empty")
	}
	if c.Partitions < 1 {
		return fmt.Errorf("topics: %q: partitions must be >= 1", c.Name)
	}
	if c.ReplicationFactor < 1 {
		return fmt.Errorf("topics: %q: replication_factor must be >= 1", c.Name)
	}
	for k, v := range c.Configs {
		if k == "" {
			return fmt.Errorf("topics: %q: config key must not be empty", c.Name)
		}
		if v == "" {
			return fmt.Errorf("topics: %q: config %q must have a value", c.Name, k)
		}
	}
	return nil
}

// validateTopics проверяет каждый топик и отсутствие дублей по имени.
func validateTopics(topics []TopicConfig) error {
	seen := make(map[string]bool, len(topics))
	for _, t := range topics {
		if err := t.Validate(); err != nil {
			return err
		}
		if seen[t.Name] {
			return fmt.Errorf("topics: duplicate topic %q", t.Name)
		}
		seen[t.Name] = true
	}
	return nil
}
