package main

import (
	"context"
	"log"

	"github.com/giicoo/ecst-go/config"
	"github.com/giicoo/ecst-go/consumer"
	"github.com/giicoo/ecst-go/envelope"
	"github.com/giicoo/ecst-go/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

type User struct{ Name string }

// Ошибки опущены для краткости — в реальном коде так не надо.
func main() {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := config.Config{
		Kafka:    config.KafkaConfig{Brokers: []string{"localhost:9092"}},
		Producer: config.DefaultProducerConfig(),
		Consumer: config.DefaultConsumerConfig("example", []string{"ecst.user.v1"}),
	}

	p, _ := producer.New(cfg)
	defer p.Close()
	producer.Publish(ctx, p, "ecst.user.v1",
		envelope.New("user", "u-1", 1, envelope.OpCreate, &User{Name: "Ada"}))

	c, _ := consumer.New(cfg)
	c.Handle("user", func(_ context.Context, r *kgo.Record) error {
		var ev envelope.Envelope[User]
		ev, _ = ev.Decode(r.Value)
		log.Printf("consumed: %s v%d %+v", ev.EntityID, ev.Version, ev.Payload)
		cancel()
		return nil
	})
	c.Run(ctx)
}
