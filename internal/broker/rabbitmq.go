package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/models"
	"github.com/Guizzs26/go-sync-db/pkg/metrics"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RabbitMQClient handles the low-level communication with the message broker
type RabbitMQClient struct {
	conn       *amqp.Connection
	channel    *amqp.Channel // publishing channel
	consumeCh  *amqp.Channel // consuming channel
	logger     *slog.Logger
	connClosed chan *amqp.Error
	chanClosed chan *amqp.Error
	closeOnce  sync.Once
	healthy    atomic.Bool
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewRabbitMQClient initializes a connection and a channel, enabling Publisher Confirms by default
func NewRabbitMQClient(url string, l *slog.Logger) (*RabbitMQClient, error) {
	c, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}

	ch, err := c.Channel()
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("failed to open RabbitMQ channel: %v", err)
	}

	// Create a second channel for the Feedback Loop
	cCh, err := c.Channel()
	if err != nil {
		ch.Close()
		c.Close()
		return nil, fmt.Errorf("failed to open consuming channel: %v", err)
	}

	if err := ch.ExchangeDeclare(
		"pax.topic",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		ch.Close()
		cCh.Close()
		c.Close()
		return nil, fmt.Errorf("failed to declare topic exchange: %v", err)
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		cCh.Close()
		c.Close()
		return nil, fmt.Errorf("failed to activate Publisher Confirms: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := &RabbitMQClient{
		conn:       c,
		channel:    ch,
		consumeCh:  cCh,
		logger:     l,
		connClosed: make(chan *amqp.Error, 1),
		chanClosed: make(chan *amqp.Error, 1),
		ctx:        ctx,
		cancel:     cancel,
	}

	client.healthy.Store(true)
	metrics.HealthStatus.Set(1)

	client.conn.NotifyClose(client.connClosed)
	client.channel.NotifyClose(client.chanClosed)

	go func() {
		select {
		case err := <-client.connClosed:
			client.healthy.Store(false)
			// System is unhealthy
			metrics.HealthStatus.Set(0)
			l.Warn("RabbitMQ connection closed", "error", err)
		case err := <-client.chanClosed:
			client.healthy.Store(false)
			// System is unhealthy
			metrics.HealthStatus.Set(0)
			l.Warn("RabbitMQ channel closed", "error", err)
		case <-client.ctx.Done():
			return
		}
	}()
	l.Info("Successfully connected to RabbitMQ and monitors established", "url", url)
	return client, nil
}

// Publish sends an entry to the broker and blocks until a confirmation (ACK/NACK) is received
func (r *RabbitMQClient) Publish(ctx context.Context, routingKey string, entry models.OutboxEntry) error {
	if !r.IsHealthy() {
		return fmt.Errorf("broker connection is closed")
	}

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
		"pax.topic",
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
		l.Error("failed to publish message to exchange", "error", err)
		return fmt.Errorf("publish call failed: %v", err)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-deferred.Done():
		if !deferred.Acked() {
			return fmt.Errorf("RabbitMQ NACK received: message not persisted")
		}
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("publisher confirm timeout")
	}
}

// Consume registers a handler for a specific queue (used for feedback loop)
func (r *RabbitMQClient) Consume(ctx context.Context, queueName string, handler func(ctx context.Context, body []byte) error) error {
	msgs, err := r.consumeCh.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case d, ok := <-msgs:
				if !ok {
					return
				}
				// Start processing the dead letter
				if err := handler(ctx, d.Body); err != nil {
					r.logger.Error("Feedback: handler failed", "error", err)
					d.Nack(false, true) // Requeue for retry
					continue
				}
				d.Ack(false)
			}
		}
	}()
	return nil
}

// Close gracefully shuts down the RabbitMQ resources
func (r *RabbitMQClient) Close() error {
	r.closeOnce.Do(func() {
		r.logger.Info("Terminating RabbitMQ client")
		r.cancel()
		if r.channel != nil {
			r.channel.Close()
		}
		if r.conn != nil {
			r.conn.Close()
		}
	})
	return nil
}

// IsHealthy returns true if the connection and channel are active
func (r *RabbitMQClient) IsHealthy() bool {
	return r.healthy.Load()
}
