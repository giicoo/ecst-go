package producer

import (
	"crypto/tls"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kslog"
)

type Config struct {
	// Bootstrap servers
	Brokers  []string
	ClientID string


	//////////////
	// SECURITY //
	//////////////
	SASLUser string
	SASLPass string
	TLS      *tls.Config


	/////////////////
	// RELIABILITY //
	/////////////////
	
	// Общее время "жизни" записи (сумма всех ретраев) 
	RecordDeliveryTimeout time.Duration

	// Время ожидание ACKS от брокера
	ProduceRequestTimeout time.Duration 

	// Время на подключение к брокеру
	DialTimeout           time.Duration

	
	////////////
	// MEMORY //
	////////////

	// Время наполения батча до его отправки
	Linger        time.Duration

	// Максимальный размер батча (отправляется при достижении)
	BatchMaxBytes int32

	// Буффер - место в памяти куда складываются записи
	// после Produce, пока брокер недоступен
	//
	// Максимальное кол-во записей в буффере клиента
	MaxBufferedRecords int

	// Максимальный объем буффера клиента
	MaxBufferedBytes   int

}

// DefaultConfig возвращает конфиг с разумными значениями по умолчанию.
// Нужные поля можно переопределить после вызова.
func DefaultConfig(brokers ...string) Config {
	return Config{
		Brokers:  brokers,
		ClientID: "ecst-producer",

		RecordDeliveryTimeout: 15 * time.Second,
		ProduceRequestTimeout: 5 * time.Second,
		DialTimeout:           5 * time.Second,

		Linger:        2 * time.Millisecond,
		BatchMaxBytes: 1 << 20, // 1 MiB, не больше message.max.bytes на брокере

		MaxBufferedRecords: 50_000,
		MaxBufferedBytes:   256 << 20, // 256 MiB
	}
}

// ValidateProducer проверяет конфиг и возвращает все найденные проблемы разом.
func (c Config) ValidateProducer() error {
	var errs []error
	add := func(msg string) { errs = append(errs, errors.New("config: "+msg)) }

	if len(c.Brokers) == 0 {
		add("brokers are required")
	}

	// Без RecordDeliveryTimeout запись ретраится вечно, поэтому он обязателен
	if c.RecordDeliveryTimeout <= 0 {
		add("RecordDeliveryTimeout must be > 0")
	}
	if c.ProduceRequestTimeout <= 0 {
		add("ProduceRequestTimeout must be > 0")
	}
	if c.DialTimeout <= 0 {
		add("DialTimeout must be > 0")
	}

	if c.Linger < 0 {
		add("Linger must be >= 0")
	}
	if c.RecordDeliveryTimeout > 0 && c.RecordDeliveryTimeout <= c.Linger {
		add("RecordDeliveryTimeout must be > Linger")
	}

	if c.BatchMaxBytes <= 0 {
		add("BatchMaxBytes must be > 0")
	}
	if c.MaxBufferedRecords <= 0 {
		add("MaxBufferedRecords must be > 0")
	}
	if c.MaxBufferedBytes <= 0 {
		add("MaxBufferedBytes must be > 0")
	}
	if c.MaxBufferedBytes > 0 && c.BatchMaxBytes > 0 && c.MaxBufferedBytes < int(c.BatchMaxBytes) {
		add("MaxBufferedBytes must be >= BatchMaxBytes")
	}

	if (c.SASLUser == "") != (c.SASLPass == "") {
		add("SASLUser and SASLPass must be set together")
	}

	return errors.Join(errs...)
}

// Перевод [Config] в [kgo.Opt]
func (c Config) ProducerOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(c.ClientID),
		kgo.DialTimeout(c.DialTimeout),

		// Значение по умолчанию: Ждем подтверждение вес реплик
		kgo.RequiredAcks(kgo.AllISRAcks()),

		kgo.RecordDeliveryTimeout(c.RecordDeliveryTimeout),
		kgo.ProduceRequestTimeout(c.ProduceRequestTimeout),

		// Батчинг и сжатие
		kgo.ProducerLinger(c.Linger),
		kgo.ProducerBatchMaxBytes(c.BatchMaxBytes),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.SnappyCompression()),

		// Backpressure: при заполнении буфера Produce блокируется
		kgo.MaxBufferedRecords(c.MaxBufferedRecords),
		kgo.MaxBufferedBytes(c.MaxBufferedBytes),

		kgo.WithLogger(kslog.New(slog.Default())),
	}

	if c.TLS != nil {
		opts = append(opts, kgo.DialTLSConfig(c.TLS))
	}
	if c.SASLUser != "" {
		opts = append(opts, kgo.SASL(
			scram.Auth{User: c.SASLUser, Pass: c.SASLPass}.AsSha512Mechanism(),
		))
	}

	return opts
}

// Дефолтный конфиг
func (c Config) WithProducerDefaults() Config {
	if c.ClientID == "" {
		c.ClientID = "ecst-producer"
	}
	if c.RecordDeliveryTimeout == 0 {
		c.RecordDeliveryTimeout = 15 * time.Second
	}
	if c.ProduceRequestTimeout == 0 {
		c.ProduceRequestTimeout = 5 * time.Second
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.Linger == 0 {
		c.Linger = 2 * time.Millisecond
	}
	if c.BatchMaxBytes == 0 {
		c.BatchMaxBytes = 1 << 20 
	}
	if c.MaxBufferedRecords == 0 {
		c.MaxBufferedRecords = 50_000
	}
	if c.MaxBufferedBytes == 0 {
		c.MaxBufferedBytes = 256 << 20 // 256 MiB
	}
	return c
}
