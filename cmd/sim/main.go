package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"event-sim/internal/config"
	"event-sim/internal/producer"
	"event-sim/internal/simulator"
	"event-sim/internal/sink"
)

func main() {
	var cfgFile string
	var verbose bool

	rootCmd := &cobra.Command{
		Use:   "sim",
		Short: "E-commerce event stream simulator",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cfgFile, verbose)
		},
	}

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "configs/config.yaml", "config file path")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose (development) logging")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(cfgFile string, verbose bool) error {
	var logger *zap.Logger
	var err error
	if verbose {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer logger.Sync()

	cfg, err := config.Load(cfgFile)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	var sinks []sink.Sink
	for _, s := range cfg.Simulator.Sinks {
		switch s {
		case "stdout":
			sinks = append(sinks, sink.NewStdoutSink())
		case "file":
			fs, err := sink.NewFileSink(cfg.Simulator.OutputFile)
			if err != nil {
				return fmt.Errorf("file sink: %w", err)
			}
			sinks = append(sinks, fs)
		default:
			logger.Warn("unknown sink type, skipping", zap.String("sink", s))
		}
	}

	prod := producer.New(
		cfg.Kafka.Brokers,
		cfg.Kafka.Topic,
		cfg.Kafka.BatchSize,
		cfg.Kafka.BatchTimeout,
		cfg.Kafka.Compression,
		logger,
	)
	defer prod.Close()

	if err := initTopic(cfg.Kafka.Brokers, cfg.Kafka.Topic, logger); err != nil {
		return fmt.Errorf("init topic: %w", err)
	}

	sim, err := simulator.New(cfg, prod, sinks, logger)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		if err := http.ListenAndServe(cfg.Simulator.MetricsAddr, mux); err != nil {
			logger.Error("metrics server error", zap.Error(err))
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("shutting down...")
		cancel()
	}()

	logger.Info("simulator started",
		zap.Int("concurrent_users", cfg.Simulator.ConcurrentUsers),
		zap.Float64("events_per_second", cfg.Simulator.EventsPerSecond),
		zap.Int64("seed", cfg.Simulator.Seed),
		zap.String("topic", cfg.Kafka.Topic),
		zap.Strings("brokers", cfg.Kafka.Brokers),
	)

	return sim.Run(ctx)
}

func initTopic(brokers []string, topic string, logger *zap.Logger) error {
	var lastErr error
	for i := 0; i < 30; i++ {
		conn, err := kafka.Dial("tcp", brokers[0])
		if err != nil {
			lastErr = err
			logger.Warn("waiting for broker", zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}

		controller, err := conn.Controller()
		conn.Close()
		if err != nil {
			lastErr = err
			logger.Warn("waiting for controller", zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}

		addr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
		c, err := kafka.Dial("tcp", addr)
		if err != nil {
			lastErr = err
			logger.Warn("waiting for controller connect", zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}

		err = c.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		c.Close()
		if err != nil {
			lastErr = err
			logger.Warn("waiting for topic creation", zap.Error(err))
			time.Sleep(2 * time.Second)
			continue
		}

		logger.Info("topic ready", zap.String("topic", topic))
		return nil
	}
	return fmt.Errorf("topic init failed after 30 retries: %w", lastErr)
}
