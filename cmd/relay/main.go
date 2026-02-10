package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/broker"
	"github.com/Guizzs26/go-sync-db/internal/db"
	"github.com/Guizzs26/go-sync-db/internal/service"
	"github.com/Guizzs26/go-sync-db/pkg/infra"
)

func main() {
	logger := infra.SetupLogger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbURL := getEnv("DATABASE_URL", "postgres://admin:password@localhost:5432/modern_pax_db")
	mqURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

	postgres, err := db.NewPostgresRepository(ctx, dbURL)
	if err != nil {
		slog.Error("falha fatal no Postgres", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()

	rabbitmq, err := broker.NewRabbitMQClient(mqURL, logger)
	if err != nil {
		slog.Error("falha fatal no RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitmq.Close()

	syncService := service.NewSyncService(postgres, rabbitmq, logger)

	dlqDone := make(chan struct{})
	go runMaintenance(ctx, postgres, dlqDone)

	slog.Info("🚀 Sentinel Relay Service iniciado", "pid", os.Getpid())

	runMainLoop(ctx, syncService, dlqDone)
}

func runMainLoop(ctx context.Context, s *service.SyncService, dlqDone chan struct{}) {
	backoff := service.NewBackoff(1*time.Second, 60*time.Second, 2.0)

	for {
		select {
		case <-ctx.Done():
			slog.Info("👋 Encerrando Relay Service de forma graciosa...")
			<-dlqDone
			slog.Info("✅ Shutdown concluído")
			return

		default:
			if err := s.ProcessNextBatch(ctx); err != nil {
				wait := backoff.Next()
				slog.Error("erro crítico no lote",
					"attempt", backoff.Attempts(),
					"retry_in", wait,
					"error", err,
				)

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

func runMaintenance(ctx context.Context, repo *db.PostgresRepository, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			slog.Info("🧹 Sweep: Iniciando limpeza da Outbox (Moving to DLQ)")
			if err := repo.MoveToDLQ(ctx); err != nil {
				slog.Error("falha na manutenção da DLQ", "error", err)
			}
		case <-ctx.Done():
			slog.Info("🛑 Parando goroutine de manutenção")
			return
		}
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
