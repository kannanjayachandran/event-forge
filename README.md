# EventForge

> #### Synthetic Event Simulation Platform for Data Engineering

EventForge generates realistic synthetic e-commerce activity for testing data pipelines, streaming systems, analytics platforms, and data infrastructure.

Instead of replaying static datasets, EventForge simulates user behavior through configurable state machines and produces coherent event streams suitable for Kafka, data lakes, warehouses, and analytics workloads.

## Why EventForge?

Building and testing data systems often requires large volumes of realistic event data. Production data is usually unavailable due to privacy, compliance, or operational constraints.

EventForge provides:

* Realistic user journey simulation
* Deterministic replay using seeded generation
* Configurable traffic patterns and behavior
* Kafka-native event streaming
* Local development and benchmarking environments
* Containerized deployment with observability

Typical use cases:

* Kafka and Redpanda testing
* Stream processing validation
* Data pipeline development
* Analytics engineering
* Warehouse benchmarking
* Dashboard prototyping
* Load and performance testing

---

## Features

### Event Simulation

* Configurable e-commerce state machine
* Session-based user journeys
* State-specific event payloads
* Deterministic replay via configurable seeds

Current flow:

```text
landing
  ↓
search
  ↓
product_view
  ↓
add_to_cart
  ↓
checkout
  ↓
purchase
```

### Streaming

* Kafka / Redpanda integration
* Asynchronous batching
* Compression support

  * Snappy
  * Gzip
  * LZ4
  * ZSTD

### Outputs

* Kafka
* NDJSON files
* Stdout

### Observability

* Prometheus metrics
* Grafana dashboards
* Real-time event monitoring

### Deployment

* Docker Compose environment
* Redpanda broker
* Prometheus
* Grafana

---

## Architecture

```text
                 ┌─────────────┐
                 │   Config    │
                 └──────┬──────┘
                        │
                        ▼
              ┌──────────────────┐
              │ Simulation Engine│
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │ Event Generation │
              └────────┬─────────┘
                       │
         ┌─────────────┼─────────────┐
         │             │             │
         ▼             ▼             ▼
      Kafka         File         Stdout
```

---

## Quick Start

### Local Development

```bash
go run ./cmd/sim -c configs/config.yaml
```

### Docker

```bash
docker compose up -d
```

Services:

| Service    | Address                |
| ---------- | ---------------------- |
| Redpanda   | localhost:19092        |
| Metrics    | localhost:8080/metrics |
| Prometheus | localhost:9090         |
| Grafana    | localhost:3000         |

Grafana credentials:

```text
admin / admin
```

---

## Configuration

Example:

```yaml
kafka:
  brokers:
    - "localhost:19092"
  topic: "ecommerce-events"
  batch_size: 100
  batch_timeout: "1s"
  compression: "snappy"

simulator:
  seed: 42
  concurrent_users: 20
  events_per_second: 25

  sinks:
    - stdout
    - file

  output_file: "/var/log/sim/events.ndjson"

state_machine:
  transitions:
    - from: landing
      to: search
      probability: 0.8
```

Environment variable overrides:

```text
SIM_KAFKA_BROKERS
SIM_SIMULATOR_SINKS
```

---

## Event Schema

```json
{
  "event_id": "uuid",
  "timestamp": "2024-01-15T10:30:00Z",
  "session_id": "uuid",
  "user_id": "uuid",
  "event_type": "search",
  "data": {}
}
```

---

## Metrics

EventForge exposes Prometheus metrics including:

```text
sim_events_produced_total
sim_sessions_created_total
sim_active_users
sim_kafka_messages_sent_total
sim_kafka_batches_sent_total
```

---

## Testing

```bash
go test ./...

./scripts/validate-transitions.sh

./scripts/test-acceptance.sh
```

---

## Current Status

### Phase 1 Complete

* State-machine driven event generation
* Configurable transition probabilities
* Deterministic simulation
* Kafka producer with async batching
* File and stdout sinks
* Prometheus metrics
* Grafana dashboards
* Docker Compose deployment

### Planned

* Persistent users and personas
* Temporal traffic patterns
* Historical/backfill generation
* Multi-domain support
* Data quality anomaly injection
* Source-system simulation
