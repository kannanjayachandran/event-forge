package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"event-sim/internal/model"
)

func TestLoadDefaultConfig(t *testing.T) {
	cfg, err := Load("../../configs/config.yaml")
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, int64(42), cfg.Simulator.Seed)
	assert.Equal(t, 100, cfg.Simulator.ConcurrentUsers)
	assert.Equal(t, 200.0, cfg.Simulator.EventsPerSecond)
	assert.Equal(t, ":8080", cfg.Simulator.MetricsAddr)
	assert.ElementsMatch(t, []string{"stdout", "file"}, cfg.Simulator.Sinks)

	assert.Equal(t, "exponential", cfg.Simulator.Timing.Distribution)
	assert.Equal(t, 0.04, cfg.Simulator.Timing.Alpha)

	assert.Len(t, cfg.Kafka.Brokers, 1)
	assert.Equal(t, "ecommerce-events", cfg.Kafka.Topic)
	assert.Equal(t, 100, cfg.Kafka.BatchSize)
	assert.Equal(t, "snappy", cfg.Kafka.Compression)

	assert.Len(t, cfg.Products, 60)
	assert.Equal(t, "prod-001", cfg.Products[0].ID)
	assert.Equal(t, 79.99, cfg.Products[0].Price)

	assert.Len(t, cfg.SearchQueries, 99)
	assert.Equal(t, "wireless headphones", cfg.SearchQueries[0])

	assert.Len(t, cfg.StateMachine.Transitions, 8)
	assert.Equal(t, "landing", string(cfg.StateMachine.Transitions[0].From))
	assert.Equal(t, "search", string(cfg.StateMachine.Transitions[0].To))
	assert.Equal(t, 0.6, cfg.StateMachine.Transitions[0].Probability)
}

func validConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			Brokers:      []string{"localhost:19092"},
			Topic:        "ecommerce-events",
			BatchSize:    100,
			BatchTimeout: 500_000_000, // 500ms
			Compression:  "snappy",
		},
		StateMachine: StateMachineConfig{
			Transitions: []model.TransitionRule{
				{From: model.StateLanding, To: model.StateSearch, Probability: 0.8},
			},
		},
		Simulator: SimulatorConfig{
			Seed:            42,
			ConcurrentUsers: 10,
			EventsPerSecond: 25,
			SessionDuration: 60_000_000_000, // 60s
			Sinks:           []string{"stdout"},
			OutputFile:      "/tmp/events.ndjson",
			MetricsAddr:     ":8080",
			Timing: TimingConfig{
				Distribution: "exponential",
				Alpha:        0.04,
				Mu:           0.0,
				Sigma:        1.0,
			},
		},
		Products: []Product{
			{ID: "prod-001", Name: "Headphones", Category: "electronics", Price: 79.99},
		},
		SearchQueries: []string{"headphones"},
	}
}

func TestValidateValidConfig(t *testing.T) {
	cfg := validConfig()
	assert.NoError(t, cfg.Validate())
}

func TestUseKafkaTrue(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "kafka"}
	assert.True(t, cfg.UseKafka())
}

func TestUseKafkaFalse(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "file"}
	assert.False(t, cfg.UseKafka())
}

func TestValidateConfigWithoutKafkaSkipsKafkaValidation(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "file"}
	cfg.Kafka.Brokers = nil
	cfg.Kafka.Topic = ""
	cfg.Kafka.BatchSize = -1
	cfg.Kafka.BatchTimeout = -1
	cfg.Kafka.Compression = "invalid"
	assert.NoError(t, cfg.Validate())
}

func TestValidateEmptyBrokers(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "kafka"}
	cfg.Kafka.Brokers = nil
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka.brokers")
}

func TestValidateEmptyTopic(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "kafka"}
	cfg.Kafka.Topic = ""
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka.topic")
}

func TestValidateNegativeBatchSize(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "kafka"}
	cfg.Kafka.BatchSize = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka.batch_size")
}

func TestValidateNegativeBatchTimeout(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "kafka"}
	cfg.Kafka.BatchTimeout = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka.batch_timeout")
}

func TestValidateInvalidCompression(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "kafka"}
	cfg.Kafka.Compression = "rar"
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka.compression")
}

func TestValidateEmptyCompressionIsValid(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "kafka"}
	cfg.Kafka.Compression = ""
	assert.NoError(t, cfg.Validate())
}

func TestValidateValidCompressionTypes(t *testing.T) {
	for _, c := range []string{"snappy", "gzip", "lz4", "zstd"} {
		cfg := validConfig()
		cfg.Simulator.Sinks = []string{"stdout", "kafka"}
		cfg.Kafka.Compression = c
		assert.NoError(t, cfg.Validate(), "compression %q should be valid", c)
	}
}

func TestValidateUnknownSink(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "elasticsearch"}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.sinks")
}

func TestValidateDuplicateSink(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "stdout"}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate")
}

func TestValidateConcurrentUsersZero(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.ConcurrentUsers = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.concurrent_users")
}

func TestValidateConcurrentUsersNegative(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.ConcurrentUsers = -5
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.concurrent_users")
}

func TestValidateEventsPerSecondZero(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.EventsPerSecond = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.events_per_second")
}

func TestValidateEventsPerSecondNegative(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.EventsPerSecond = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.events_per_second")
}

func TestValidateSessionDurationZero(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.SessionDuration = 0
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.session_duration")
}

func TestValidateSessionDurationNegative(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.SessionDuration = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.session_duration")
}

func TestValidateMetricsAddrEmpty(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.MetricsAddr = ""
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.metrics_addr")
}

func TestValidateInvalidTimingDistribution(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Timing.Distribution = "uniform"
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.timing.distribution")
}

func TestValidateEmptyTimingDistributionIsValid(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Timing.Distribution = ""
	assert.NoError(t, cfg.Validate())
}

func TestValidateAlphaNegative(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Timing.Alpha = -0.1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.timing.alpha")
}

func TestValidateSigmaNegative(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Timing.Sigma = -1
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "simulator.timing.sigma")
}

func TestValidateEmptyProducts(t *testing.T) {
	cfg := validConfig()
	cfg.Products = nil
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "products")
}

func TestValidateEmptySearchQueries(t *testing.T) {
	cfg := validConfig()
	cfg.SearchQueries = nil
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "search_queries")
}

func TestValidateEmptyTransitions(t *testing.T) {
	cfg := validConfig()
	cfg.StateMachine.Transitions = nil
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "state_machine.transitions")
}

func TestValidateTransitionProbabilityTooHigh(t *testing.T) {
	cfg := validConfig()
	cfg.StateMachine.Transitions = []model.TransitionRule{
		{From: model.StateLanding, To: model.StateSearch, Probability: 1.5},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "probability")
}

func TestValidateTransitionProbabilityNegative(t *testing.T) {
	cfg := validConfig()
	cfg.StateMachine.Transitions = []model.TransitionRule{
		{From: model.StateLanding, To: model.StateSearch, Probability: -0.1},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "probability")
}

func TestValidateTransitionProbabilitiesSumExceedsOne(t *testing.T) {
	cfg := validConfig()
	cfg.StateMachine.Transitions = []model.TransitionRule{
		{From: model.StateLanding, To: model.StateSearch, Probability: 0.6},
		{From: model.StateLanding, To: model.StateProductView, Probability: 0.5},
	}
	err := cfg.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sum")
}

func TestValidateReturnsMultipleErrors(t *testing.T) {
	cfg := validConfig()
	cfg.Simulator.Sinks = []string{"stdout", "kafka"}
	cfg.Kafka.Brokers = nil
	cfg.Simulator.ConcurrentUsers = 0
	cfg.Products = nil

	err := cfg.Validate()
	require.Error(t, err)

	msg := err.Error()
	assert.Contains(t, msg, "kafka.brokers")
	assert.Contains(t, msg, "simulator.concurrent_users")
	assert.Contains(t, msg, "products")
}
