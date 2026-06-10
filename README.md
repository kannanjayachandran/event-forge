# Event Forge

Modern Event Simulation Platform for Data Engineering

## Overview

Event Forge is a high-performance event stream simulator that generates synthetic e-commerce events. It simulates user journeys through a configurable state machine, producing realistic event streams for testing data pipelines, stream processing systems, and analytics platforms.

## Features

- **State Machine Simulation**: Users transition through 6 states: `landing` → `search` → `product_view` → `add_to_cart` → `checkout` → `purchase`
- **Configurable Transitions**: Probabilities for state transitions defined in YAML config
- **Kafka Integration**: Async batched writes with compression support (snappy, gzip, lz4, zstd)
- **Multiple Sinks**: Output to stdout and/or file (NDJSON format)
- **Prometheus Metrics**: Built-in metrics endpoint for monitoring event production
- **Docker Ready**: Full docker-compose setup with Redpanda (Kafka-compatible), Prometheus, and Grafana

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌───────────┐
│  Config     │────▶│  Simulator  │────▶│  Sinks    │
│             │     │             │     │ (stdout/  │
└─────────────┘     └──────┬──────┘     │  file)    │
                           │            └───────────┘
                           │
                           ▼
                    ┌─────────────┐
                    │  Producer   │
                    │  (kafka)    │
                    └─────────────┘
```

## Quick Start

### Run Locally

```bash
go run ./cmd/sim -c configs/config.yaml
```

### Run with Docker

```bash
docker-compose up -d
```

This starts:
- Redpanda broker at `localhost:19092`
- Simulator with metrics at `localhost:8080/metrics`
- Prometheus at `localhost:9090`
- Grafana at `localhost:3000` (admin/admin)

## Configuration

```yaml
kafka:
  brokers: ["localhost:19092"]
  topic: "ecommerce-events"
  batch_size: 100
  batch_timeout: "1s"
  compression: "snappy"

state_machine:
  transitions:
    - from: "landing"
      to: "search"
      probability: 0.8

simulator:
  seed: 42
  concurrent_users: 20
  events_per_second: 25
  sinks: ["stdout", "file"]
  output_file: "/var/log/sim/events.ndjson"
  metrics_addr: ":8080"
```

Environment variables (prefixed with `SIM_`):
- `SIM_KAFKA_BROKERS` - Comma-separated broker list
- `SIM_SIMULATOR_SINKS` - Comma-separated sink list

## Event Schema

```json
{
  "event_id": "uuid",
  "timestamp": "2024-01-15T10:30:00Z",
  "session_id": "uuid",
  "user_id": "uuid",
  "event_type": "search",
  "data": { /* state-specific payload */ }
}
```

## Metrics

- `sim_events_produced_total{event_type}` - Events per state
- `sim_sessions_created_total` - Total user sessions
- `sim_active_users` - Concurrent user count
- `sim_kafka_messages_sent_total` - Kafka messages sent
- `sim_kafka_batches_sent_total` - Kafka batches sent

## Testing

```bash
go test ./...
./scripts/validate-transitions.sh
./scripts/test-acceptance.sh
```

## Phase 1 Complete

- [x] State machine with configurable transitions
- [x] Event generator with state-specific payloads
- [x] Kafka producer with async batching
- [x] Stdout and file sinks
- [x] Prometheus metrics integration
- [x] Docker-compose infrastructure
- [x] Grafana dashboards