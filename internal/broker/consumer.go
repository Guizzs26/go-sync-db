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

// Listen starts the consumption loop and configures DLQs for poison messages
func (c *RabbitMQConsumer) Listen(ctx context.Context) error {
	exchangeName := "pax.topic"
	dlxExchange := "pax.dlx" // Exchange for "dead" messages

	queueName := fmt.Sprintf("pax.queue.unit.%d", c.unitID)
	dlqName := fmt.Sprintf("pax.queue.unit.%d.dead", c.unitID)

	routingKey := fmt.Sprintf("pax.unit.%d.#", c.unitID)

	// Declare Dead Letter Exchange
	if err := c.channel.ExchangeDeclare(dlxExchange, "topic", true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare DLX: %v", err)
	}

	// Declare Dead Letter Queue (Where "poison pills" are stored)
	if _, err := c.channel.QueueDeclare(dlqName, true, false, false, false, nil); err != nil {
		return fmt.Errorf("failed to declare DLQ: %v", err)
	}

	// Bind DLQ to DLX
	if err := c.channel.QueueBind(dlqName, routingKey, dlxExchange, false, nil); err != nil {
		return fmt.Errorf("failed to bind DLQ: %v", err)
	}

	// Declare Main Queue with Dead Lettering configuration
	// x-dead-letter-exchange: instructs RabbitMQ to send Nacks (requeue=false) to this exchange
	q, err := c.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange": dlxExchange,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to declare main queue: %v", err)
	}

	if err := c.channel.QueueBind(q.Name, routingKey, exchangeName, false, nil); err != nil {
		return fmt.Errorf("failed to bind main queue: %v", err)
	}

	msgs, err := c.channel.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %v", err)
	}

	c.logger.Info("Consumer is online with DLQ protection",
		"queue", q.Name,
		"dlq", dlqName,
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
				c.logger.Error("MALFORMED ENVELOPE: Failed to unmarshal - sending to DLQ", "error", err)

				metrics.ConsumerMessages.WithLabelValues("fatal", unitIDStr).Inc()

				// Nack with requeue=false triggers automatic routing to DLQ
				if nackErr := d.Nack(false, false); nackErr != nil {
					c.logger.Error("CRITICAL: Failed to nack malformed message", "nack_error", nackErr)
				}
				c.backoff.Reset()
				continue
			}

			msgCtx, msgCancel := context.WithTimeout(ctx, 30*time.Second)
			c.logger.Debug("Processing message", "correlation_id", entry.CorrelationID)

			err := c.handler.ProcessMessage(msgCtx, entry)
			msgCancel()

			if err != nil {
				// Fatal Error Detection (Poison Pill Prevention)
				if strings.HasPrefix(err.Error(), "FATAL:") {
					metrics.ConsumerMessages.WithLabelValues("fatal", unitIDStr).Inc()

					c.logger.Error("NON-RECOVERABLE ERROR: Moving message to DLQ",
						"correlation_id", entry.CorrelationID,
						"error", err,
					)

					// x-dead-letter-exchange handles routing to the .dead queue
					if nackErr := d.Nack(false, false); nackErr != nil {
						c.logger.Error("CRITICAL: Failed to nack fatal message", "nack_error", nackErr)
					}
					c.backoff.Reset()
				} else {
					// Transient Error (Network/Connection) - Retry with Backoff
					metrics.ConsumerMessages.WithLabelValues("retry_queue", unitIDStr).Inc()

					wait := c.backoff.Next()
					c.logger.Warn("TRANSIENT ERROR: Requeueing",
						"correlation_id", entry.CorrelationID,
						"wait", wait,
						"error", err,
					)

					time.Sleep(wait)

					if nackErr := d.Nack(false, true); nackErr != nil {
						c.logger.Error("CRITICAL: Failed to requeue message", "nack_error", nackErr)
					}
				}
				continue
			}

			metrics.ConsumerMessages.WithLabelValues("success", unitIDStr).Inc()
			c.backoff.Reset()

			if err := d.Ack(false); err != nil {
				c.logger.Error("CRITICAL: ACK failed", "correlation_id", entry.CorrelationID, "error", err)
			}
		}
	}
}

// Close gracefully terminates RabbitMQ resources
func (c *RabbitMQConsumer) Close() {
	c.logger.Info("Shutting down RabbitMQ consumer")
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}
