package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/models"
	"github.com/Guizzs26/go-sync-db/internal/processor"
	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQConsumer manages the connection and message flow from the broker
type RabbitMQConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	handler *processor.SyncHandler
	logger  *slog.Logger
	unitID  int
}

// NewRabbitMQConsumer initializes the consumer with a specific Unit ID for routing
func NewRabbitMQConsumer(url string, unitID int, handler *processor.SyncHandler, logger *slog.Logger) (*RabbitMQConsumer, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}

	// QoS: Prefetch 1 ensures we process messages one by one, maintaining strict order
	if err := ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("failed to set QoS: %v", err)
	}

	return &RabbitMQConsumer{
		conn:    conn,
		channel: ch,
		handler: handler,
		logger:  logger,
		unitID:  unitID,
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

	c.logger.Info("Consumer is online and waiting for messages", "queue", q.Name, "routing_key", routingKey)

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-msgs:
			if !ok {
				return fmt.Errorf("message channel closed")
			}

			var entry models.OutboxEntry
			if err := json.Unmarshal(d.Body, &entry); err != nil {
				c.logger.Error("Failed to unmarshal message", "error", err)
				d.Nack(false, false) // Drop malformed messages
				continue
			}

			// Core synchronization execution
			err := c.handler.ProcessMessage(ctx, entry)
			if err != nil {
				c.logger.Error("Processing failed, requeueing", "correlation_id", entry.CorrelationID, "error", err)
				time.Sleep(5 * time.Second) // Throttling retries
				d.Nack(false, true)         // Requeue for another attempt
				continue
			}

			// Manual Ack: Only confirmed after successful Firebird commit
			if err := d.Ack(false); err != nil {
				c.logger.Error("Failed to Ack message", "correlation_id", entry.CorrelationID, "error", err)
			}
		}
	}
}

// Close gracefully terminates RabbitMQ resources
func (c *RabbitMQConsumer) Close() {
	c.logger.Info("Shutting down RabbitMQ consumer")
	c.channel.Close()
	c.conn.Close()
}
