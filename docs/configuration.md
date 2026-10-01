# Конфигурация

Все конфиги имеют `DefaultConfig`/`Default` с разумными значениями — нужные поля переопределяют
после вызова. Валидация возвращает **все** найденные проблемы разом.

## `backoff.Config`

```go
backoff.Default() // Min 250ms, Max 5s, Factor 2, Jitter 1
```

| Поле | По умолчанию | Смысл |
|---|---|---|
| `Min` | `250ms` | задержка перед первой повторной попыткой |
| `Max` | `5s` | потолок задержки |
| `Factor` | `2` | во сколько раз растет задержка |
| `Jitter` | `1` | доля задержки, размываемая случайно, `[0, 1]` |

`Jitter: 1` — full jitter, задержка от 0 до расчетной. Нужен, чтоб инстансы, упавшие на одной
ошибке, не пошли ретраить одновременно. `0` выключает джиттер — так делать не стоит.

## `producer.Config`

```go
cfg := producer.DefaultConfig("localhost:9092")
```

### Надежность

| Поле | По умолчанию | Смысл |
|---|---|---|
| `Acks` | `AcksAll` | сколько подтверждений ждать |
| `RecordDeliveryTimeout` | `15s` | общее время жизни записи со всеми ретраями |
| `ProduceRequestTimeout` | `5s` | ожидание ack на один запрос |
| `DialTimeout` | `5s` | подключение к брокеру |
| `Backoff` | `backoff.Default()` | задержка между ретраями |

`AcksAll` — единственный режим, при котором работает идемпотентная запись; `AcksLeader` и `AcksNone`
выключают ее принудительно (требование Kafka, иначе `kgo.NewClient` вернет ошибку).

`RecordDeliveryTimeout` обязателен и должен быть `> Linger`: без него запись ретраится вечно, а
`Produce` с отвязанным от отмены контекстом зависнет навсегда.

### Батчинг и память

| Поле | По умолчанию | Смысл |
|---|---|---|
| `Linger` | `2ms` | время наполнения батча перед отправкой |
| `BatchMaxBytes` | `1 MiB` | размер батча; **не больше `message.max.bytes` брокера** |
| `Compression` | `[zstd, snappy]` | кодеки в порядке предпочтения, брокер выберет первый поддерживаемый |
| `MaxBufferedRecords` | `50 000` | записей в буфере клиента |
| `MaxBufferedBytes` | `256 MiB` | объем буфера; должен быть `>= BatchMaxBytes` |

При заполнении буфера `Produce` блокируется — это backpressure, а не потеря.

### Безопасность

`SASLUser` + `SASLPass` (SCRAM-SHA-512, выставляются только вместе) и `TLS *tls.Config`.

## `consumer.Config`

```go
cfg := consumer.DefaultConfig("localhost:9092")
cfg.Group = "orders-consumer"   // обязательно
cfg.Topics = []string{"orders"} // обязательно
cfg.DLQ = dlq                   // обязательно
```

| Поле | По умолчанию | Смысл |
|---|---|---|
| `Group` | — | consumer group, внутри которой делятся партиции |
| `Topics` | — | что читаем |
| `DLQ` | — | куда уезжают необработанные записи |
| `StartOffset` | `earliest` | откуда читать, если у группы нет коммита или он вышел за retention |
| `HandlerMaxAttempts` | `3` | попыток обработать запись; `1` — без повторов |
| `DLQMaxAttempts` | `3` | попыток положить в DLQ |
| `CommitTimeout` | `5s` | ожидание подтверждения коммита |
| `SessionTimeout` | `45s` | без heartbeat за это время брокер считает консьюмера мертвым |
| `RebalanceTimeout` | `60s` | сколько группа ждет консьюмеров на ребалансе |
| `FetchMaxWait` | `500ms` | сколько брокер ждет наполнения фетча |
| `FetchMaxBytes` | `50 MiB` | объем одного фетча |
| `MaxPollRecords` | `500` | записей за один `Poll`; это же размер батча между коммитами |

`DLQ` обязательна: без нее необработанную запись некуда деть, и она встанет поперек своей партиции
намертво — ребаланс и рестарт приведут нового владельца к той же записи.

`RebalanceTimeout` должен быть больше времени обработки одного батча: ребаланс заблокирован на время
раздачи (`BlockRebalanceOnPoll`). Если не укладываетесь — снижайте `MaxPollRecords`.

`StartOffsetEarliest` — полный реплей при новой группе. `StartOffsetLatest` — только новое; для
ECST обычно не то, что нужно: получатель не соберет свою копию.

## `ecst.OutboxConfig`

```go
cfg := ecst.DefaultOutboxConfig(store, "localhost:9092")
```

| Поле | По умолчанию | Смысл |
|---|---|---|
| `Store` | — | ваша реализация `OutboxStore` |
| `Producer` | `producer.DefaultConfig`, ClientID `ecst-outbox` | свой продюсер публикации |
| `BatchSize` | `100` | записей за один `Fetch` |
| `PollInterval` | `1s` | пауза, когда таблица пуста; полный батч → без паузы |
| `BatchTimeout` | `30s` | таймаут на весь заход вместе с ретраями к БД |
| `StoreMaxAttempts` | `3` | попыток на один запрос к `OutboxStore` |
| `Backoff` | `backoff.Default()` | задержка между попытками к БД |

## `ecst.InboxConfig`

```go
cfg := ecst.DefaultInboxConfig("orders-consumer", "localhost:9092")
cfg.Register(ecst.Topic("orders", 2, handleOrder)) // минимум одна регистрация
```

| Поле | По умолчанию | Смысл |
|---|---|---|
| `Consumer` | `consumer.DefaultConfig` + `Group` | базовый конфиг; `Topics` и `DLQ` выставляет сам Inbox |
| `Producer` | `producer.DefaultConfig`, ClientID `ecst-dlq` | продюсер под DLQ-записи |
| `HandlerTimeout` | `30s` | таймаут на один вызов хендлера |
| `PoolSize` | `1` | консьюмеров на топик по умолчанию |
| `DLQTopic` | `topic + ".dlq"` | функция имени DLQ-топика |

`ClientID` консьюмеров собирается как `<ClientID>-<topic>-<index>` — видно в метриках брокера, кто
именно читает.

## Готовые наборы

**Низкая латентность, мелкие события**

```go
p := producer.DefaultConfig(brokers...)
p.Linger = 0
p.BatchMaxBytes = 256 << 10
```

**Высокая пропускная, лаг терпим**

```go
p.Linger = 50 * time.Millisecond
out.BatchSize = 1000
out.PollInterval = 200 * time.Millisecond
```

**Тяжелый хендлер (внешние вызовы)**

```go
c.MaxPollRecords = 50          // батч должен успеть в RebalanceTimeout
c.RebalanceTimeout = 5 * time.Minute
c.SessionTimeout = 60 * time.Second
inbox.HandlerTimeout = 2 * time.Minute
```

**Прод с SASL/TLS**

```go
p.SASLUser, p.SASLPass = user, pass
p.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
c.SASLUser, c.SASLPass = user, pass
c.TLS = p.TLS
```
