package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Guizzs26/go-sync-db/internal/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQClient handles the low-level communication with the message broker
type RabbitMQClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *slog.Logger
}

// NewRabbitMQClient initializes a connection and a channel, enabling Publisher Confirms by default
func NewRabbitMQClient(url string, l *slog.Logger) (*RabbitMQClient, error) {
	c, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := c.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %v", err)
	}

	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("failed to activate Publisher Confirms: %v", err)
	}

	l.Info("Successfully connected to RabbitMQ", "url", url)
	return &RabbitMQClient{
		conn:    c,
		channel: ch,
		logger:  l,
	}, nil
}

// Publish sends an entry to the broker and blocks until a confirmation (ACK/NACK) is received
func (r *RabbitMQClient) Publish(ctx context.Context, routingKey string, entry models.OutboxEntry) error {
	body, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to serialize entry: %v", err)
	}

	l := r.logger.With(
		"correlation_id", entry.CorrelationID,
		"routing_key", routingKey,
	)

	deferred, err := r.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		"pax.direct",
		routingKey,
		false,
		false,
		amqp.Publishing{
			Headers: amqp.Table{
				"correlation_id": entry.CorrelationID,
			},
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		l.Error("RabbitMQ returned NACK (persistence failure)")
		return fmt.Errorf("RabbitMQ NACK for correlation_id: %s", entry.CorrelationID)
	}

	if !deferred.Wait() {
		l.Error("RabbitMQ returned NACK (persistence failure)")
		return fmt.Errorf("RabbitMQ NACK for correlation_id: %s", entry.CorrelationID)
	}

	l.Debug("Message confirmed by broker")
	return nil
}

// Close gracefully shuts down the RabbitMQ resources
func (r *RabbitMQClient) Close() {
	if r.channel != nil {
		r.logger.Info("Closing RabbitMQ channel")
		r.channel.Close()
	}
	if r.conn != nil {
		r.logger.Info("Disconnecting from RabbitMQ")
		r.conn.Close()
	}
}
