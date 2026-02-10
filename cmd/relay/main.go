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

	rabbitmq, err := broker.NewRabbitMQClient(cfg.RabbitMQURL, logger)
	if err != nil {
		slog.Error("Fatal error connecting to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitmq.Close()

	syncService := service.NewSyncService(postgres, rabbitmq, logger)

	dlqDone := make(chan struct{})
	go runMaintenance(ctx, postgres, cfg.MaintenanceInterval, dlqDone)

	slog.Info("🚀 Sentinel Relay Service started", "pid", os.Getpid())
	runMainLoop(ctx, syncService, cfg, dlqDone)
}

func runMainLoop(ctx context.Context, s *service.SyncService, cfg *config.Config, dlqDone chan struct{}) {
	backoff := service.NewBackoff(1*time.Second, 60*time.Second, 2.0)

	for {
		select {
		case <-ctx.Done():
			slog.Info("👋 Shutting down main loop...")
			<-dlqDone // Block until maintenance finishes current task
			slog.Info("✅ Shutdown complete")
			return
		default:
			if err := s.ProcessNextBatch(ctx, cfg.BatchSize); err != nil {
				wait := backoff.Next()
				slog.Error("Batch processing error",
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
			time.Sleep(cfg.PollInterval)
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
			slog.Info("🧹 Sweep: Starting Outbox cleanup (Moving to DLQ)")
			if err := repo.MoveToDLQ(ctx); err != nil {
				slog.Error("DLQ maintenance failure", "error", err)
			}
		case <-ctx.Done():
			slog.Info("🛑 Stopping maintenance goroutine")
			return
		}
	}
}
