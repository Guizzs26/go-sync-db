package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/models"
	"github.com/Guizzs26/go-sync-db/internal/processor"
	"github.com/Guizzs26/go-sync-db/pkg/infra"
	"github.com/Guizzs26/go-sync-db/pkg/metrics"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQConsumer manages the connection and message flow from the broker
type RabbitMQConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	handler *processor.SyncHandler
	logger  *slog.Logger
	unitID  int
	backoff *infra.Backoff
}

// NewRabbitMQConsumer initializes the consumer with a specific Unit ID for routing
func NewRabbitMQConsumer(url string, unitID int, handler *processor.SyncHandler, logger *slog.Logger) (*RabbitMQConsumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}

	// QoS: Prefetch 1 ensures we process messages one by one, maintaining strict order
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %v", err)
	}

	return &RabbitMQConsumer{
		conn:    conn,
		channel: ch,
		handler: handler,
		logger:  logger,
		unitID:  unitID,
		backoff: infra.NewBackoff(100*time.Millisecond, 30*time.Second, 2.0),
	}, nil
}

// Listen starts the consumption loop and handles the queue/exchange binding
func (c *RabbitMQConsumer) Listen(ctx context.Context) error {
	exchangeName := "pax.topic"
	queueName := fmt.Sprintf("pax.queue.unit.%d", c.unitID)
	routingKey := fmt.Sprintf("pax.unit.%d.#", c.unitID)

	// Declare Queue with durability to survive broker restarts
	q, err := c.channel.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %v", err)
	}

	// Bind queue to the specific Unit routing key
	if err := c.channel.QueueBind(q.Name, routingKey, exchangeName, false, nil); err != nil {
		return fmt.Errorf("failed to bind queue: %v", err)
	}

	msgs, err := c.channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %v", err)
	}

	c.logger.Info("Consumer is online and waiting for messages",
		"queue", q.Name,
		"routing_key", routingKey,
	)

	unitIDStr := fmt.Sprintf("%d", c.unitID)
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Stopping consumer loop due to context cancellation")
			return nil
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("message channel closed unexpectedly")
			}

			var entry models.OutboxEntry
			if err := json.Unmarshal(d.Body, &entry); err != nil {
				c.logger.Error("MALFORMED ENVELOPE: Failed to unmarshal message - dropping", "error", err)

				metrics.ConsumerMessages.WithLabelValues("fatal", unitIDStr).Inc()

				if nackErr := d.Nack(false, false); nackErr != nil {
					c.logger.Error("CRITICAL: Failed to drop malformed message", "nack_error", nackErr)
				}
				c.backoff.Reset()
				continue
			}

			// Core synchronization execution
			err := c.handler.ProcessMessage(ctx, entry)

			if err != nil {
				// Fatal Error Detection (Poison Pill Prevention)
				if strings.HasPrefix(err.Error(), "FATAL:") {
					metrics.ConsumerMessages.WithLabelValues("fatal", unitIDStr).Inc()

					c.logger.Error("NON-RECOVERABLE ERROR: Dropping message",
						"correlation_id", entry.CorrelationID,
						"error", err,
					)

					if nackErr := d.Nack(false, false); nackErr != nil {
						c.logger.Error("CRITICAL: Failed to drop fatal message",
							"correlation_id", entry.CorrelationID,
							"nack_error", nackErr,
						)
					}
					c.backoff.Reset()
				} else {
					metrics.ConsumerMessages.WithLabelValues("retry_queue", unitIDStr).Inc()

					wait := c.backoff.Next()
					attempts := c.backoff.Attempts()

					c.logger.Warn("TRANSIENT ERROR: Requeueing with backoff",
						"correlation_id", entry.CorrelationID,
						"attempt", attempts,
						"wait_duration", wait,
						"error", err,
					)

					time.Sleep(wait)

					if nackErr := d.Nack(false, true); nackErr != nil {
						c.logger.Error("CRITICAL: Failed to requeue message",
							"correlation_id", entry.CorrelationID,
							"nack_error", nackErr,
						)
					}
				}
				continue
			}
			metrics.ConsumerMessages.WithLabelValues("success", unitIDStr).Inc()
			c.backoff.Reset()
			if err := d.Ack(false); err != nil {
				c.logger.Error("CRITICAL: ACK failed - message WILL BE REDELIVERED",
					"correlation_id", entry.CorrelationID,
					"error", err,
					"note", "Idempotency logic must handle the duplicate",
				)
			}
		}
	}
}

// Close gracefully terminates RabbitMQ resources
func (c *RabbitMQConsumer) Close() {
	c.logger.Info("Shutting down RabbitMQ consumer resources")
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
