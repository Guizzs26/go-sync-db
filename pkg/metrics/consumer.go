package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ConsumerDuration tracks the end-to-end latency of processing a message inside the worker
	// We use larger buckets because Firebird 2.5 on HDDs can be slow
	ConsumerDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "consumer_processing_duration_seconds",
		Help:    "Time taken to process a message from reception to Firebird commit",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30},
	}, []string{"status", "table", "operation"}) // status: success, error, fatal

	// ConsumerMessages tracks the throughput and result of message consumption
	ConsumerMessages = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_messages_total",
		Help: "Total number of messages processed by the consumer",
	}, []string{"status", "unit_id"}) // status: success, fatal, transient

	// ConsumerRetries tracks how many times we had to retry internally due to locks
	ConsumerRetries = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "consumer_lock_retries_total",
		Help: "Number of internal retries triggered by Firebird locks/deadlocks",
	}, []string{"table"})
)
