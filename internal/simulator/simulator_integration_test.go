//go:build integration

package simulator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"go.uber.org/zap"

	"event-sim/internal/config"
	"event-sim/internal/producer"
	"event-sim/internal/sink"
)

func startRedpanda(ctx context.Context) (*redpanda.Container, string, error) {
	c, err := redpanda.Run(ctx,
		"docker.redpanda.com/redpandadata/redpanda:v24.3.7",
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		return nil, "", err
	}
	broker, err := c.KafkaSeedBroker(ctx)
	if err != nil {
		testcontainers.TerminateContainer(c)
		return nil, "", err
	}
	return c, broker, nil
}

func createTopic(t *testing.T, broker, topic string) {
	t.Helper()
	conn, err := kafka.Dial("tcp", broker)
	require.NoError(t, err)
	defer conn.Close()

	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	require.NoError(t, err, "create topic %s", topic)
}

func kafkaTestConfig(broker, topic string) *config.Config {
	cfg := defaultTestConfig()
	cfg.Simulator.Sinks = []string{"kafka"}
	cfg.Simulator.MetricsAddr = ":0"
	cfg.Kafka = config.KafkaConfig{
		Brokers:      []string{broker},
		Topic:        topic,
		BatchSize:    1,
		BatchTimeout: 100 * time.Millisecond,
		Compression:  "snappy",
	}
	return cfg
}

func runSimCollectEvents(t *testing.T, cfg *config.Config, n int, sinks []sink.Sink, prod *producer.Producer) {
	t.Helper()

	sim, err := New(cfg, prod, sinks, zap.NewNop())
	require.NoError(t, err)
	sim.minDelay = 0

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		sim.Run(ctx)
		close(done)
	}()

	timeout := time.After(30 * time.Second)
	for {
		select {
		case <-timeout:
			cancel()
			<-done
			t.Fatalf("timed out waiting for %d events", n)
		default:
		}
		if len(sinks) > 0 {
			if ss, ok := sinks[0].(*sink.SliceSink); ok && ss.Count() >= n {
				cancel()
				<-done
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func consumeKafkaMessages(t *testing.T, ctx context.Context, broker, topic string, n int) []kafka.Message {
	t.Helper()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{broker},
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
	})
	defer r.Close()

	msgs := make([]kafka.Message, 0, n)
	timeout := time.After(30 * time.Second)
	for len(msgs) < n {
		select {
		case <-timeout:
			t.Fatalf("timed out reading %d messages from Kafka, got %d", n, len(msgs))
		default:
		}
		msg, err := r.ReadMessage(ctx)
		require.NoError(t, err)
		msgs = append(msgs, msg)
	}
	return msgs
}

func TestIntegrationBasicProduceAndConsume(t *testing.T) {
	ctx := context.Background()
	c, broker, err := startRedpanda(ctx)
	require.NoError(t, err)
	defer testcontainers.TerminateContainer(c)

	topic := "integration-basic"
	cfg := kafkaTestConfig(broker, topic)
	n := 100

	createTopic(t, broker, topic)

	s := &sink.SliceSink{}
	prod := producer.New(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.BatchSize, cfg.Kafka.BatchTimeout, cfg.Kafka.Compression)
	defer prod.Close()

	runSimCollectEvents(t, cfg, n, []sink.Sink{s}, prod)

	msgs := consumeKafkaMessages(t, ctx, broker, topic, n)
	assert.Len(t, msgs, n, "all produced events should arrive in Kafka")

	for i, msg := range msgs {
		var evt struct {
			EventType string `json:"event_type"`
		}
		require.NoError(t, json.Unmarshal(msg.Value, &evt), "message %d: valid JSON", i)
		assert.NotEmpty(t, evt.EventType, "message %d: has event_type field", i)
		assert.Greater(t, len(msg.Key), 0, "message %d: has non-empty key", i)
	}

	t.Logf("successfully produced and consumed %d events via Kafka", n)
}

func TestIntegrationDeterministicReplayViaKafka(t *testing.T) {
	ctx := context.Background()
	c, broker, err := startRedpanda(ctx)
	require.NoError(t, err)
	defer testcontainers.TerminateContainer(c)

	n := 500

	// Create topics and producers upfront to avoid kafka-go DefaultTransport issue
	// where closing a Writer and creating a new one corrupts shared transport state.
	topicA, topicB := "integration-deterministic-a", "integration-deterministic-b"
	createTopic(t, broker, topicA)
	createTopic(t, broker, topicB)

	cfgA := kafkaTestConfig(broker, topicA)
	cfgA.Simulator.Seed = 42
	cfgB := kafkaTestConfig(broker, topicB)
	cfgB.Simulator.Seed = 42

	prodA := producer.New(cfgA.Kafka.Brokers, cfgA.Kafka.Topic, cfgA.Kafka.BatchSize, cfgA.Kafka.BatchTimeout, cfgA.Kafka.Compression)
	prodB := producer.New(cfgB.Kafka.Brokers, cfgB.Kafka.Topic, cfgB.Kafka.BatchSize, cfgB.Kafka.BatchTimeout, cfgB.Kafka.Compression)

	sA, sB := &sink.SliceSink{}, &sink.SliceSink{}

	runSimCollectEvents(t, cfgA, n, []sink.Sink{sA}, prodA)
	runSimCollectEvents(t, cfgB, n, []sink.Sink{sB}, prodB)

	prodA.Close()
	prodB.Close()

	msgs1 := consumeKafkaMessages(t, ctx, broker, topicA, n)
	msgs2 := consumeKafkaMessages(t, ctx, broker, topicB, n)

	types := func(msgs []kafka.Message) []string {
		tt := make([]string, len(msgs))
		for i, msg := range msgs {
			var evt struct {
				EventType string `json:"event_type"`
			}
			json.Unmarshal(msg.Value, &evt)
			tt[i] = evt.EventType
		}
		return tt
	}
	assert.Equal(t, types(msgs1), types(msgs2),
		"same seed should produce identical event type sequence in Kafka")
}

func TestIntegrationMultipleSinkModes(t *testing.T) {
	ctx := context.Background()
	c, broker, err := startRedpanda(ctx)
	require.NoError(t, err)
	defer testcontainers.TerminateContainer(c)

	topic := "integration-multi-sink"
	cfg := kafkaTestConfig(broker, topic)
	cfg.Simulator.Sinks = []string{"kafka", "stdout"}
	n := 100

	createTopic(t, broker, topic)

	s := &sink.SliceSink{}
	prod := producer.New(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.BatchSize, cfg.Kafka.BatchTimeout, cfg.Kafka.Compression)
	defer prod.Close()

	runSimCollectEvents(t, cfg, n, []sink.Sink{s}, prod)

	msgs := consumeKafkaMessages(t, ctx, broker, topic, n)
	assert.Len(t, msgs, n, "events should arrive in Kafka with stdout+kafka sinks")
	assert.GreaterOrEqual(t, s.Count(), n, "SliceSink should capture at least n events")
}
