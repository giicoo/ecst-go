// Package migrator создаёт в Kafka топики, описанные в cfg.Topics.
// Существующие топики не трогает: менять партиции и конфиги живого топика —
// работа администратора кластера, а не стартующего приложения.
package migrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/giicoo/ecst-go/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Сколько ждать появления созданного топика в метаданных кластера.
const waitTopicTimeout = 10 * time.Second

// Migrate создаёт недостающие топики. Уже существующий топик остаётся как есть,
// даже если его партиции или конфиги расходятся с описанием.
func Migrate(ctx context.Context, cfg config.Config) error {
	if err := cfg.ValidateTopics(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	client, err := kgo.NewClient(cfg.Kafka.KgoOpts()...)
	if err != nil {
		return fmt.Errorf("new kgo client: %w", err)
	}
	defer client.Close()

	adm := kadm.NewClient(client)

	names := make([]string, 0, len(cfg.Topics))
	for _, t := range cfg.Topics {
		names = append(names, t.Name)
	}

	details, err := adm.ListTopics(ctx, names...)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}

	for _, t := range cfg.Topics {
		d, ok := details[t.Name]
		switch {
		case ok && d.Err == nil:
			continue // топик уже есть — не трогаем
		case ok && !errors.Is(d.Err, kerr.UnknownTopicOrPartition):
			return fmt.Errorf("describe topic %s: %w", t.Name, d.Err)
		}

		if err := createTopic(ctx, adm, t); err != nil {
			return fmt.Errorf("create topic %s: %w", t.Name, err)
		}
	}

	return nil
}

func createTopic(ctx context.Context, adm *kadm.Client, t config.TopicConfig) error {
	configs := make(map[string]*string, len(t.Configs))
	for k, v := range t.Configs {
		configs[k] = &v
	}

	resp, err := adm.CreateTopics(ctx, t.Partitions, t.ReplicationFactor, configs, t.Name)
	if err != nil {
		return err
	}
	// топик мог появиться между ListTopics и CreateTopics — это не ошибка
	if err := resp.Error(); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return err
	}

	// метаданные о новом топике расходятся по кластеру не мгновенно
	return waitTopic(ctx, adm, t.Name)
}

// waitTopic ждёт, пока созданный топик появится в метаданных.
func waitTopic(ctx context.Context, adm *kadm.Client, name string) error {
	ctx, cancel := context.WithTimeout(ctx, waitTopicTimeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if details, err := adm.ListTopics(ctx, name); err == nil {
			if d, ok := details[name]; ok && d.Err == nil {
				return nil
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return fmt.Errorf("topic did not appear in metadata: %w", ctx.Err())
		}
	}
}
