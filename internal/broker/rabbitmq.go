package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/Guizzs26/go-sync-db/internal/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	logger  *slog.Logger
}

func NewRabbitMQClient(url string, l *slog.Logger) (*RabbitMQClient, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("falha ao conectar no RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("falha ao abrir canal: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("falha ao ativar Publisher Confirms: %w", err)
	}

	l.Info("conectado ao RabbitMQ com sucesso", "url", url)

	return &RabbitMQClient{
		conn:    conn,
		channel: ch,
		logger:  l,
	}, nil
}

func (r *RabbitMQClient) Publish(ctx context.Context, routingKey string, entry models.OutboxEntry) error {
	body, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("falha ao serializar entry: %w", err)
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
		l.Error("falha no disparo do publish", "error", err)
		return fmt.Errorf("erro ao disparar publish: %w", err)
	}

	if !deferred.Wait() {
		l.Error("RabbitMQ enviou NACK (falha de persistência)")
		return fmt.Errorf("RabbitMQ enviou NACK para correlation_id: %s", entry.CorrelationID)
	}

	l.Debug("mensagem confirmada pelo broker")

	return nil
}

func (r *RabbitMQClient) Close() {
	if r.channel != nil {
		r.logger.Info("fechando canal do RabbitMQ")
		r.channel.Close()
	}
	if r.conn != nil {
		r.logger.Info("desconectando do RabbitMQ")
		r.conn.Close()
	}
}
