// Пример ECST-сервиса: события пишутся в outbox-таблицу, оттуда уезжают
// в Kafka и возвращаются в inbox-хендлеры этого же сервиса.
//
// Нужен запущенный брокер: docker compose up -d
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/giicoo/ecst-go/ecst"
)

const (
	broker = "localhost:9092"
	group  = "ecst-example"

	ordersTopic = "orders"
	usersTopic  = "users"
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
	store := newMemStore()

	svc, err := ecst.New(ecst.Config{
		// Опция 1: outbox-воркер забирает события из таблицы
		// и публикует их своим продюсером
		Outbox: ecst.DefaultOutboxConfig(store, broker),

		// Опция 2: inbox-воркер поднимает пул консьюмеров на каждый топик
		Inbox: inbox(),
	})
	if err != nil {
		return err
	}
	defer svc.Close(ctx)

	// Бизнес-логика: пишет события в свою БД, про Kafka ничего не знает
	go emitOrders(ctx, store)

	svc.Run(ctx)

	return nil
}

func inbox() *ecst.InboxConfig {
	cfg := ecst.DefaultInboxConfig(group, broker)

	// ecst.Topic разбирает конверт за хендлер: битое событие уедет в DLQ
	// сразу, без ретраев. Второй аргумент - размер пула консьюмеров
	// на топик, 0 - взять InboxConfig.PoolSize
	cfg.Register(
		ecst.Topic(ordersTopic, 2, handleOrder),
		ecst.Topic(usersTopic, 0, handleUser),
	)

	return cfg
}
