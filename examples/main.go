// Пример использования ecst-go: подготовить топик, опубликовать события
// по сущности user и прочитать их консьюмером.
package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"github.com/giicoo/ecst-go/config"
	"github.com/giicoo/ecst-go/consumer"
	"github.com/giicoo/ecst-go/envelope"
	"github.com/giicoo/ecst-go/migrator"
	"github.com/giicoo/ecst-go/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic      = "ecst.user.v1"
	entityType = "user"
	groupID    = "example"
)

type User struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	cfg := config.Config{
		Kafka:    config.DefaultKafkaConfig("example-app", "localhost:9092"),
		Producer: config.DefaultProducerConfig(),
		Consumer: config.DefaultConsumerConfig(groupID, []string{topic}),
		Topics:   []config.TopicConfig{config.DefaultTopicConfig(topic)},
	}

	// топик приводится к описанному виду до старта клиентов
	if err := migrator.Migrate(ctx, cfg); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	events := []envelope.Envelope[User]{
		envelope.New(entityType, "u-1", 1, envelope.OpCreate, &User{Name: "Ada", Email: "ada@example.com"}),
		envelope.New(entityType, "u-1", 2, envelope.OpUpdate, &User{Name: "Ada Lovelace", Email: "ada@example.com"}),
		// OpDelete публикуется как tombstone: value=nil, ключ остаётся
		envelope.New[User](entityType, "u-2", 1, envelope.OpDelete, nil),
	}

	p, err := producer.New(cfg)
	if err != nil {
		log.Fatalf("new producer: %v", err)
	}
	defer p.Close()

	for _, ev := range events {
		if err := producer.Publish(ctx, p, topic, ev); err != nil {
			log.Fatalf("publish %s v%d: %v", ev.EntityID, ev.Version, err)
		}
	}
	if err := p.Shutdown(ctx); err != nil {
		log.Fatalf("shutdown producer: %v", err)
	}
	log.Printf("published %d events to %s", len(events), topic)

	// группа читает с начала топика, поэтому консьюмер стартует после публикации
	c, err := consumer.New(cfg)
	if err != nil {
		log.Fatalf("new consumer: %v", err)
	}
	defer c.Close()

	// хендлер выбирается по заголовку envelope_type
	c.Handle(entityType, func(_ context.Context, r *kgo.Record) error {
		if len(r.Value) == 0 { // tombstone: идентификатор удалённой сущности в ключе
			log.Printf("deleted: %s", r.Key)
		} else {
			var ev envelope.Envelope[User]
			ev, err := ev.Decode(r.Value)
			if err != nil {
				// ошибка наверх — запись не коммитится и будет перечитана
				return fmt.Errorf("decode record at %s/%d: %w", r.Topic, r.Partition, err)
			}
			log.Printf("consumed: %s v%d op=%s %+v", ev.EntityID, ev.Version, ev.Op, *ev.Payload)
		}
		return nil
	})

	// Run читает, пока не отменят контекст: Ctrl+C или SIGTERM. Штатная
	// остановка — не ошибка, возвращается nil.
	if err := c.Run(ctx); err != nil {
		log.Fatalf("consume: %v", err)
	}
	log.Print("done")
}
