package config

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/viper"

	"event-sim/internal/model"
)

type Config struct {
	Kafka         KafkaConfig        `mapstructure:"kafka"`
	StateMachine  StateMachineConfig `mapstructure:"state_machine"`
	Simulator     SimulatorConfig    `mapstructure:"simulator"`
	Products      []Product          `mapstructure:"products"`
	SearchQueries []string           `mapstructure:"search_queries"`
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
	Timing          TimingConfig  `mapstructure:"timing"`
}

type TimingConfig struct {
	Distribution string  `mapstructure:"distribution"`
	Alpha        float64 `mapstructure:"alpha"`
	Mu           float64 `mapstructure:"mu"`
	Sigma        float64 `mapstructure:"sigma"`
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

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	var errs []error

	errs = append(errs, c.validateSinks()...)
	errs = append(errs, c.validateKafka()...)
	errs = append(errs, c.validateSimulator()...)
	errs = append(errs, c.validateStateMachine()...)
	errs = append(errs, c.validateProducts()...)
	errs = append(errs, c.validateSearchQueries()...)

	return errors.Join(errs...)
}

// UseKafka returns true when at least one configured sink is "kafka".
func (c *Config) UseKafka() bool {
	for _, s := range c.Simulator.Sinks {
		if s == "kafka" {
			return true
		}
	}
	return false
}

func (c *Config) validateSinks() []error {
	var errs []error
	seen := make(map[string]bool, len(c.Simulator.Sinks))
	for _, s := range c.Simulator.Sinks {
		if seen[s] {
			errs = append(errs, fmt.Errorf("simulator.sinks: duplicate sink %q", s))
		}
		seen[s] = true
		switch s {
		case "stdout", "file", "kafka":
		default:
			errs = append(errs, fmt.Errorf(
				"simulator.sinks: unsupported sink %q; must be one of: stdout, file, kafka", s,
			))
		}
	}
	return errs
}

func (c *Config) validateKafka() []error {
	var errs []error

	if !c.UseKafka() {
		return nil
	}

	if len(c.Kafka.Brokers) == 0 {
		errs = append(errs, fmt.Errorf("kafka.brokers: must specify at least one broker"))
	}
	if c.Kafka.Topic == "" {
		errs = append(errs, fmt.Errorf("kafka.topic: must not be empty"))
	}
	if c.Kafka.BatchSize < 0 {
		errs = append(errs, fmt.Errorf("kafka.batch_size: must not be negative (got %d)", c.Kafka.BatchSize))
	}
	if c.Kafka.BatchTimeout < 0 {
		errs = append(errs, fmt.Errorf("kafka.batch_timeout: must not be negative (got %s)", c.Kafka.BatchTimeout))
	}
	if c.Kafka.Compression != "" {
		switch c.Kafka.Compression {
		case "snappy", "gzip", "lz4", "zstd":
		default:
			errs = append(errs, fmt.Errorf(
				"kafka.compression: unsupported %q; must be one of: snappy, gzip, lz4, zstd",
				c.Kafka.Compression,
			))
		}
	}

	return errs
}

func (c *Config) validateSimulator() []error {
	var errs []error

	if c.Simulator.SessionDuration <= 0 {
		errs = append(errs, fmt.Errorf(
			"simulator.session_duration: must be positive (got %s)", c.Simulator.SessionDuration,
		))
	}
	if c.Simulator.ConcurrentUsers <= 0 {
		errs = append(errs, fmt.Errorf(
			"simulator.concurrent_users: must be positive (got %d)", c.Simulator.ConcurrentUsers,
		))
	}
	if c.Simulator.EventsPerSecond <= 0 {
		errs = append(errs, fmt.Errorf(
			"simulator.events_per_second: must be positive (got %g)", c.Simulator.EventsPerSecond,
		))
	}
	if c.Simulator.MetricsAddr == "" {
		errs = append(errs, fmt.Errorf("simulator.metrics_addr: must not be empty"))
	}

	dist := c.Simulator.Timing.Distribution
	if dist != "" {
		switch dist {
		case "exponential", "lognormal":
		default:
			errs = append(errs, fmt.Errorf(
				"simulator.timing.distribution: unsupported %q; must be one of: exponential, lognormal",
				dist,
			))
		}
	}
	if c.Simulator.Timing.Alpha < 0 {
		errs = append(errs, fmt.Errorf("simulator.timing.alpha: must not be negative (got %g)", c.Simulator.Timing.Alpha))
	}
	if c.Simulator.Timing.Sigma < 0 {
		errs = append(errs, fmt.Errorf("simulator.timing.sigma: must not be negative (got %g)", c.Simulator.Timing.Sigma))
	}

	return errs
}

func (c *Config) validateStateMachine() []error {
	var errs []error

	if len(c.StateMachine.Transitions) == 0 {
		errs = append(errs, fmt.Errorf("state_machine.transitions: must specify at least one transition"))
	}

	cumulative := make(map[model.State]float64)
	for i, t := range c.StateMachine.Transitions {
		prefix := fmt.Sprintf("state_machine.transitions[%d]", i)
		if t.Probability < 0 || t.Probability > 1.0 {
			errs = append(errs, fmt.Errorf(
				"%s.probability: must be between 0 and 1 (got %g)", prefix, t.Probability,
			))
		}
		cumulative[t.From] += t.Probability
	}
	for from, total := range cumulative {
		if total > 1.0+1e-9 {
			errs = append(errs, fmt.Errorf(
				"state_machine: transitions from %q sum to %.6f, must be <= 1.0",
				from, total,
			))
		}
	}

	return errs
}

func (c *Config) validateProducts() []error {
	if len(c.Products) == 0 {
		return []error{fmt.Errorf("products: must specify at least one product")}
	}
	return nil
}

func (c *Config) validateSearchQueries() []error {
	if len(c.SearchQueries) == 0 {
		return []error{fmt.Errorf("search_queries: must specify at least one search query")}
	}
	return nil
}
