package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// MessagesProcessed tracks the total throughput of the relay
	// Labels allow filtering by status (sent/error), origin (unit_id), and table
	MessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_messages_processed_total",
		Help: "Total number of messages processed by the relay service",
	}, []string{"status", "unit_id", "table"})

	// BatchDuration measures how long it takes to process an entire batch
	// Use this to identify performance degradation in Postgres or RabbitMQ
	BatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "relay_batch_duration_seconds",
		Help:    "Duration of batch processing in seconds",
		Buckets: prometheus.DefBuckets,
	})

	// BatchSize tracks the number of messages actually captured in each batch
	BatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "relay_batch_size",
		Help:    "Number of messages processed per batch",
		Buckets: []float64{1, 10, 50, 100, 500, 1000},
	})

	// RabbitMQReconnections counts how many times the service had to restore the link
	// Frequent increments indicate network instability between Sede and Cloud
	RabbitMQReconnections = promauto.NewCounter(prometheus.CounterOpts{
		Name: "relay_rabbitmq_reconnections_total",
		Help: "Total number of RabbitMQ reconnection attempts",
	})

	// HealthStatus provides a binary 0/1 signal for the service's health
	// 1 = Healthy, 0 = Unhealthy (Connection to RabbitMQ is down)
	HealthStatus = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "relay_healthy",
		Help: "Current health status of the relay (1 for healthy, 0 for unhealthy)",
	})

	// OutboxBacklog tracks the total number of pending messages in the source DB
	// This is the primary indicator of system lag
	OutboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "relay_outbox_backlog",
		Help: "Current number of pending/processing messages in the outbox table",
	})

	// DLQSize tracks the number of "poison pills" that reached maximum retries
	// If this number grows, manual intervention in the database is required
	DLQSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "relay_dlq_size",
		Help: "Current number of messages in the Dead Letter Queue",
	})
)
