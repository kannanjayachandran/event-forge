package metrics

import (
	"event-sim/internal/model"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

func init() {
	for _, s := range model.AllStates {
		EventsDropped.WithLabelValues(string(s)).Add(0)
	}
}

var (
	EventsProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sim_events_produced_total",
			Help: "Total number of events produced by event type.",
		},
		[]string{"event_type"},
	)

	SessionsCreated = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sim_sessions_created_total",
			Help: "Total number of user sessions created.",
		},
	)

	ActiveUsers = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "sim_active_users",
			Help: "Current number of active user sessions.",
		},
	)

	EventDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "sim_event_duration_seconds",
			Help:    "Time taken to produce and sink events.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"event_type"},
	)

	KafkaMessagesSent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sim_kafka_messages_sent_total",
			Help: "Total number of Kafka messages sent (individual).",
		},
	)

	KafkaBatchesSent = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "sim_kafka_batches_sent_total",
			Help: "Total number of Kafka batches sent.",
		},
	)

	EventsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "sim_events_dropped_total",
		Help: "Events dropped due to producer send failure or timeout.",
	}, []string{"event_type"})
)
