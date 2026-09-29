package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/giicoo/ecst-go/consumer"
	"github.com/giicoo/ecst-go/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	broker   = "localhost:9092"
	topic    = "orders"
	dlqTopic = "orders.dlq"
	group    = "ecst-example"
)

func main() {
	// Ctrl+C / SIGTERM отменяют ctx - это сигнал к штатной остановке
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		fmt.Println("run:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	p, err := producer.NewProducer(producer.DefaultConfig(broker))
	if err != nil {
		return err
	}
	// Тот же продюсер пишет и в DLQ, поэтому закрывается последним.
	// Close флашит буффер, поэтому контекст без отмены:
	// зависнуть не даст RecordDeliveryTimeout
	defer p.Close(context.WithoutCancel(ctx))

	p.Produce(ctx, &kgo.Record{
		Topic: topic,
		Key:   []byte("order-1"),
		Value: []byte("hello world"),
	})

	return consume(ctx, p)
}

func consume(ctx context.Context, p *producer.Producer) error {
	// DLQ обязательна: без нее запись, которую не удалось обработать,
	// встала бы поперек своей партиции
	dlq, err := consumer.NewKafkaDLQ(p, dlqTopic)
	if err != nil {
		return err
	}

	cfg := consumer.DefaultConfig(broker)
	cfg.Group = group
	cfg.Topics = []string{topic}
	cfg.DLQ = dlq

	c, err := consumer.NewConsumer(cfg, printRecord)
	if err != nil {
		return err
	}
	defer c.Close()

	return c.Run(ctx)
}

func printRecord(_ context.Context, r *kgo.Record) error {
	fmt.Printf("%s[%d]@%d %s = %s\n", r.Topic, r.Partition, r.Offset, r.Key, r.Value)
	return nil
}
