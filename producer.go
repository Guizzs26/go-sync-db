package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}

	if err = ch.Confirm(false); err != nil {
		log.Fatal(err)
	}

	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	if err = ch.ExchangeDeclare(
		"orders.v1.topic",
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		log.Fatal(err)
	}

	order := Order{
		ID:        "PED-ERRO",
		Customer:  "Pax Primavera",
		Amount:    2500.75,
		Status:    "pending",
		Timestamp: time.Now().Format(time.RFC3339),
	}
	body, _ := json.Marshal(order)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := ch.PublishWithContext(
		ctx,
		"orders.v1.topic",
		"order.created",
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         []byte(body),
		},
	); err != nil {
		log.Fatal(err)
	}

	if confirmed := <-confirms; confirmed.Ack {
		log.Printf(" [OK] Pedido %s confirmado pelo Broker no disco!", order.ID)
	} else {
		log.Printf(" [ERRO] Mensagem %s não foi confirmada (Nack do Broker)", order.ID)
	}
}
