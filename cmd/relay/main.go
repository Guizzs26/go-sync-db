package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/broker"
	"github.com/Guizzs26/go-sync-db/internal/db"
	"github.com/Guizzs26/go-sync-db/internal/service"
)

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://admin:password@localhost:5432/modern_pax_db"
	}

	mqURL := os.Getenv("RABBITMQ_URL")
	if mqURL == "" {
		mqURL = "amqp://guest:guest@localhost:5672/"
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	postgres, err := db.NewPostgresRepository(ctx, dbURL)
	if err != nil {
		log.Fatalf("❌ Erro no Postgres: %v", err)
	}
	defer postgres.Close()

	rabbitmq, err := broker.NewRabbitMQClient(mqURL)
	if err != nil {
		log.Fatalf("❌ Erro no RabbitMQ: %v", err)
	}
	defer rabbitmq.Close()

	syncService := service.NewSyncService(postgres, rabbitmq)

	log.Println("🚀 Relay Service iniciado. Monitorando pg_sync_outbox...")

	for {
		select {
		case <-ctx.Done():
			log.Println("👋 Encerrando Relay Service de forma graciosa...")
			return
		default:
			// 1. Tenta processar um lote de mensagens
			err := syncService.ProcessNextBatch(ctx)

			if err != nil {
				log.Printf("⚠️ Erro crítico: %v", err)
				time.Sleep(5 * time.Second) // Backoff para não floodar o log
				continue
			}

			// 2. Intervalo entre pollings (ajuste conforme necessidade)
			// Se o lote anterior estava cheio, poderíamos até pular o sleep
			time.Sleep(1 * time.Second)
		}
	}
}
