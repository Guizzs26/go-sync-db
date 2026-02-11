package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	MessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "relay_messages_total",
		Help: "Total messages processed by the relay",
	}, []string{"status", "table"})

	BatchDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "relay_batch_duration_seconds",
		Help: "Time spent processing a single batch",
	})

	OutboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "relay_outbox_backlog",
		Help: "Current pending messages in Postgres",
	})
)
