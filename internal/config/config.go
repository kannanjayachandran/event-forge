package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"

	"event-sim/internal/model"
)

type Config struct {
	Kafka         KafkaConfig         `mapstructure:"kafka"`
	StateMachine  StateMachineConfig  `mapstructure:"state_machine"`
	Simulator     SimulatorConfig     `mapstructure:"simulator"`
	Products      []Product           `mapstructure:"products"`
	SearchQueries []string            `mapstructure:"search_queries"`
}

type KafkaConfig struct {
	Brokers      []string      `mapstructure:"brokers"`
	Topic        string        `mapstructure:"topic"`
	BatchSize    int           `mapstructure:"batch_size"`
	BatchTimeout time.Duration `mapstructure:"batch_timeout"`
	Compression  string        `mapstructure:"compression"`
}

type StateMachineConfig struct {
	Transitions []model.TransitionRule `mapstructure:"transitions"`
}

type SimulatorConfig struct {
	Seed            int64         `mapstructure:"seed"`
	ConcurrentUsers int           `mapstructure:"concurrent_users"`
	EventsPerSecond float64       `mapstructure:"events_per_second"`
	SessionDuration time.Duration `mapstructure:"session_duration"`
	Sinks           []string      `mapstructure:"sinks"`
	OutputFile      string        `mapstructure:"output_file"`
	MetricsAddr     string        `mapstructure:"metrics_addr"`
}

type Product struct {
	ID       string  `mapstructure:"id"`
	Name     string  `mapstructure:"name"`
	Category string  `mapstructure:"category"`
	Price    float64 `mapstructure:"price"`
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetEnvPrefix("SIM")
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	sliceEnvOverrides := []struct {
		envVar string
		key    string
	}{
		{"SIM_KAFKA_BROKERS", "kafka.brokers"},
		{"SIM_SIMULATOR_SINKS", "simulator.sinks"},
	}
	for _, eo := range sliceEnvOverrides {
		if val := os.Getenv(eo.envVar); val != "" {
			v.Set(eo.key, strings.Split(val, ","))
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return &cfg, nil
}
