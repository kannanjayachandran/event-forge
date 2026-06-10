package producer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"event-sim/internal/metrics"
	"event-sim/internal/model"
)

type Producer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

func New(brokers []string, topic string, batchSize int, batchTimeout time.Duration, compression string, logger *zap.Logger) *Producer {
	var compress kafka.Compression
	switch compression {
	case "snappy":
		compress = kafka.Snappy
	case "gzip":
		compress = kafka.Gzip
	case "lz4":
		compress = kafka.Lz4
	case "zstd":
		compress = kafka.Zstd
	}

	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    batchSize,
		BatchTimeout: batchTimeout,
		Compression:  compress,
		Async:        true,
		RequiredAcks: kafka.RequireOne,
		Completion: func(messages []kafka.Message, err error) {
			metrics.KafkaBatchesSent.Inc()
			metrics.KafkaMessagesSent.Add(float64(len(messages)))
			if err != nil {
				logger.Error("kafka write error", zap.Error(err))
			}
		},
	}

	return &Producer{writer: w, logger: logger}
}

func (p *Producer) Send(ctx context.Context, event model.Event) error {
	b, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.SessionID),
		Value: b,
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(string(event.EventType))},
		},
	}

	return p.writer.WriteMessages(ctx, msg)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
