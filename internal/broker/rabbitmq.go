package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Guizzs26/go-sync-db/internal/models"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	conn     *amqp.Connection
	channel  *amqp.Channel
	confirms chan amqp.Confirmation
}

func NewRabbitMQClient(url string) (*RabbitMQClient, error) {
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

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	return &RabbitMQClient{
		conn:     conn,
		channel:  ch,
		confirms: confirms,
	}, nil
}

func (r *RabbitMQClient) Publish(ctx context.Context, routingKey string, entry models.OutboxEntry) error {
	body, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("falha ao serializar entry: %w", err)
	}

	if err = r.channel.PublishWithContext(
		ctx,
		"pax.direct", // Exchange nominal que definimos no setup
		routingKey,   // pax.unit.{id}.{table}.{op}
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			Headers: amqp.Table{
				"correlation_id": entry.CorrelationID, // Rastreabilidade total
			},
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // MENSAGEM PERSISTENTE (Fase 03)
			Body:         body,
		},
	); err != nil {
		return fmt.Errorf("erro ao disparar publish: %w", err)
	}

	// BLOQUEIO SEGURO: Espera o recibo do RabbitMQ (Publisher Confirm)
	select {
	case confirmed := <-r.confirms:
		if confirmed.Ack {
			return nil // Sucesso absoluto! O dado tocou o disco do broker.
		}
		return fmt.Errorf("RabbitMQ enviou NACK para correlation_id: %s", entry.CorrelationID)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close garante o fechamento limpo das conexões
func (r *RabbitMQClient) Close() {
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil {
		r.conn.Close()
	}
}
