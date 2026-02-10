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

	dlqDone := make(chan struct{})
	go func() {
		defer close(dlqDone)
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				log.Println("Sweep: Iniciando limpeza da Outbox (Moving to DLQ)...")
				if err := postgres.MoveToDLQ(ctx); err != nil {
					log.Printf("⚠️ Erro na manutenção da DLQ: %v", err)
				}
			case <-ctx.Done():
				log.Println("🛑 Parando goroutine da DLQ...")
				return
			}
		}
	}()

	log.Println("🚀 Relay Service iniciado. Monitorando pg_sync_outbox...")

	backoff := service.NewBackoff(1*time.Second, 60*time.Second, 2.0)
	for {
		select {
		case <-ctx.Done():
			log.Println("👋 Encerrando...")
			<-dlqDone
			return
		default:
			err := syncService.ProcessNextBatch(ctx)

			if err != nil {
				wait := backoff.Next()
				log.Printf("⚠️ Erro crítico: %v. Retrying in %v", err, wait)

				select {
				case <-time.After(wait):
					continue
				case <-ctx.Done():
					return
				}
			}
			backoff.Reset()

			time.Sleep(1 * time.Second)
		}
	}
}
