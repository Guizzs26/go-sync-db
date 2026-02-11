package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/broker"
	"github.com/Guizzs26/go-sync-db/internal/config"
	"github.com/Guizzs26/go-sync-db/internal/db"
	"github.com/Guizzs26/go-sync-db/internal/service"
	"github.com/Guizzs26/go-sync-db/pkg/infra"
)

func main() {
	cfg := config.Load()
	logger := infra.SetupLogger(cfg)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	postgres, err := db.NewPostgresRepository(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		slog.Error("Fatal error connecting to Postgres", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()

	dlqDone := make(chan struct{})
	go runMaintenance(ctx, postgres, cfg.MaintenanceInterval, dlqDone)

	slog.Info("🚀 Sentinel Relay Service started", "pid", os.Getpid())

	runMainLoop(ctx, postgres, cfg, dlqDone)
}

func runMainLoop(ctx context.Context, repo *db.PostgresRepository, cfg *config.Config, dlqDone chan struct{}) {
	backoff := service.NewBackoff(1*time.Second, 60*time.Second, 2.0)
	var rabbitmq *broker.RabbitMQClient
	var syncService *service.SyncService

	for {
		select {
		case <-ctx.Done():
			slog.Info("👋 Shutting down main loop...")
			if rabbitmq != nil {
				rabbitmq.Close()
			}
			<-dlqDone // Espera a limpeza terminar
			slog.Info("✅ Shutdown complete")
			return
		default:
			// 1. Lifecycle: Garante que o Broker está operante
			if rabbitmq == nil || !rabbitmq.IsHealthy() {
				if rabbitmq != nil {
					rabbitmq.Close()
				}

				// Tentativa de conexão (incluindo a primeira do boot)
				newRabbit, err := broker.NewRabbitMQClient(cfg.RabbitMQURL, slog.Default())
				if err != nil {
					wait := backoff.Next()
					slog.Error("RabbitMQ link failure, retrying", "wait", wait, "error", err)

					select {
					case <-time.After(wait):
						continue // Tenta novamente após o backoff
					case <-ctx.Done():
						return
					}
				}

				slog.Info("RabbitMQ link established 🚀")
				rabbitmq = newRabbit
				backoff.Reset()
				// Recria o serviço para injetar o novo cliente saudável
				syncService = service.NewSyncService(repo, rabbitmq, slog.Default())
			}

			// Execution: Processa o lote
			if err := syncService.ProcessNextBatch(ctx, cfg.BatchSize); err != nil {
				wait := backoff.Next()
				slog.Error("Batch processing error", "retry_in", wait, "error", err)

				select {
				case <-time.After(wait):
					continue // Mantém o backoff se falhar
				case <-ctx.Done():
					return
				}
			}

			// 4. Sucesso: Reseta backoff e aguarda próximo ciclo (PollInterval)
			backoff.Reset()

			select {
			case <-time.After(cfg.PollInterval):
				continue
			case <-ctx.Done():
				return
			}
		}
	}
}

func runMaintenance(ctx context.Context, repo *db.PostgresRepository, interval time.Duration, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			slog.Info("🧹 Janitor: Starting structural health checks")

			affected, err := repo.ResetStaleMessages(ctx, 10)
			if err != nil {
				slog.Error("Janitor: Failed to reset stale messages", "error", err)
			} else if affected > 0 {
				slog.Warn("Janitor: Rescued stuck messages", "count", affected)
			}

			if err := repo.MoveToDLQ(ctx); err != nil {
				slog.Error("Janitor: DLQ maintenance failure", "error", err)
			}

		case <-ctx.Done():
			slog.Info("🛑 Janitor: Stopping maintenance goroutine")
			return
		}
	}
}
