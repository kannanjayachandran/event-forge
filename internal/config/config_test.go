package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadDefaultConfig(t *testing.T) {
	cfg, err := Load("../../configs/config.yaml")
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, int64(42), cfg.Simulator.Seed)
	assert.Equal(t, 20, cfg.Simulator.ConcurrentUsers)
	assert.Equal(t, 25.0, cfg.Simulator.EventsPerSecond)
	assert.Equal(t, ":8080", cfg.Simulator.MetricsAddr)
	assert.ElementsMatch(t, []string{"stdout", "file"}, cfg.Simulator.Sinks)

	assert.Equal(t, "exponential", cfg.Simulator.Timing.Distribution)
	assert.Equal(t, 0.04, cfg.Simulator.Timing.Alpha)

	assert.Len(t, cfg.Kafka.Brokers, 1)
	assert.Equal(t, "ecommerce-events", cfg.Kafka.Topic)
	assert.Equal(t, 100, cfg.Kafka.BatchSize)
	assert.Equal(t, "snappy", cfg.Kafka.Compression)

	assert.Len(t, cfg.Products, 10)
	assert.Equal(t, "prod-001", cfg.Products[0].ID)
	assert.Equal(t, 79.99, cfg.Products[0].Price)

	assert.Len(t, cfg.SearchQueries, 15)
	assert.Equal(t, "wireless headphones", cfg.SearchQueries[0])

	assert.Len(t, cfg.StateMachine.Transitions, 8)
	assert.Equal(t, "landing", string(cfg.StateMachine.Transitions[0].From))
	assert.Equal(t, "search", string(cfg.StateMachine.Transitions[0].To))
	assert.Equal(t, 0.8, cfg.StateMachine.Transitions[0].Probability)
}
