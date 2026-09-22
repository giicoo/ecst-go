package main

import (
	"context"
	"log/slog"

	"github.com/giicoo/ecst-go/producer"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cfg := producer.DefaultConfig("localhost:9092")

	producer, err := producer.NewProducer(cfg)
	if err != nil {
		slog.Error("producer", "err", err)
	}

	ctx := context.Background()

	slog.Info("Produce...")
	r := &kgo.Record{
			Topic: "da",
			Key: []byte("da"),
			Value: []byte("hello world"),
		}
	producer.Produce(ctx, r)

	for {}
}
