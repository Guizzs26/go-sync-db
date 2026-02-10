package main

import (
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

	if err = ch.Qos(1, 0, false); err != nil {
		log.Fatal(err)
	}

	args := amqp.Table{
		"x-queue-type": "quorum",
	}

	q, err := ch.QueueDeclare(
		"orders.sync.firebird",
		true,
		false,
		false,
		false,
		args,
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := ch.QueueBind(
		q.Name,            // fila
		"order.#",         // binding_key
		"orders.v1.topic", // exchange
		false,
		nil,
	); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"sync-consumer",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	var forever chan struct{}
	go func() {
		for d := range msgs {
			var order Order
			_ = json.Unmarshal(d.Body, &order)

			log.Printf(" [⬇️] Processando Pedido: %s", order.ID)

			success := processOrder(order)

			if success {
				log.Printf(" [✅] Pedido %s sincronizado com sucesso!", order.ID)
				d.Ack(false)
			} else {
				log.Printf(" [❌] Falha crítica no pedido %s. Enviando para DLX...", order.ID)
				// Requeue: false -> Aciona a Dead Letter Exchange configurada na Policy
				d.Nack(false, false)
			}
		}
	}()

	log.Printf(" [*] Aguardando mensagens. Para sair pressione CTRL+C")
	<-forever
}

func processOrder(o Order) bool {
	time.Sleep(2 * time.Second)

	if o.ID == "PED-ERRO" {
		return false
	}
	return true
}
