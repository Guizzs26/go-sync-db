package main

import (
	"context"
	"errors"
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
	defer infra.CloseLogger()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	postgres, err := db.NewPostgresRepository(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		slog.Error("Fatal: failed to connect to Postgres", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()

	maintenanceDone := make(chan struct{})
	go runMaintenance(ctx, postgres, cfg.MaintenanceInterval, logger, maintenanceDone)

	slog.Info("🚀 Sentinel Relay Service started", "pid", os.Getpid(), "batch_size", cfg.BatchSize)

	runMainLoop(ctx, postgres, cfg, logger, maintenanceDone)
}

// runMainLoop handles the synchronization lifecycle and infrastructure resilience
func runMainLoop(ctx context.Context, repo *db.PostgresRepository, cfg *config.Config, logger *slog.Logger, maintenanceDone chan struct{}) {
	backoff := service.NewBackoff(1*time.Second, 60*time.Second, 2.0)
	var rabbitmq *broker.RabbitMQClient
	var syncService *service.SyncService

	for {
		select {
		case <-ctx.Done():
			logger.Info("👋 Graceful shutdown initiated: stopping main loop")
			if rabbitmq != nil {
				rabbitmq.Close()
			}
			<-maintenanceDone // Wait for janitor to finish its last cycle
			logger.Info("✅ Sentinel Relay reached safe state. Farewell.")
			return

		default:
			// A. Infrastructure Health Check
			if rabbitmq == nil || !rabbitmq.IsHealthy() {
				if rabbitmq != nil {
					rabbitmq.Close()
				}

				logger.Info("Attempting to establish RabbitMQ link...")
				newRabbit, err := broker.NewRabbitMQClient(cfg.RabbitMQURL, logger)
				if err != nil {
					wait := backoff.Next()
					logger.Error("RabbitMQ link failure, backing off", "wait", wait, "error", err)

					select {
					case <-time.After(wait):
						continue
					case <-ctx.Done():
						return
					}
				}

				logger.Info("RabbitMQ link established 🚀")
				rabbitmq = newRabbit
				backoff.Reset()
				syncService = service.NewSyncService(repo, rabbitmq, logger)
			}

			// B. Batch Processing Execution
			if err := syncService.ProcessNextBatch(ctx, cfg.BatchSize); err != nil {
				// We don't backoff if the context was cancelled (expected shutdown)
				if errors.Is(err, context.Canceled) {
					continue
				}

				wait := backoff.Next()
				logger.Error("Batch processing error", "retry_in", wait, "error", err)

				select {
				case <-time.After(wait):
					continue
				case <-ctx.Done():
					return
				}
			}

			// C. Success Path: Reset backoff and wait for next poll interval
			backoff.Reset()

			select {
			case <-time.After(cfg.PollInterval):
				// Ready for next cycle
			case <-ctx.Done():
				// Interrupted during wait
			}
		}
	}
}

// runMaintenance manages stale messages and moves poison pills to the DLQ
func runMaintenance(ctx context.Context, repo *db.PostgresRepository, interval time.Duration, logger *slog.Logger, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	logger.Debug("Janitor: Maintenance worker is online")

	for {
		select {
		case <-ticker.C:
			logger.Info("🧹 Janitor: Starting structural health checks")

			// Recover messages stuck in 'processing' status
			affected, err := repo.ResetStaleMessages(ctx, 10)
			if err != nil {
				logger.Error("Janitor: Failed to reset stale messages", "error", err)
			} else if affected > 0 {
				logger.Warn("Janitor: Rescued stuck messages", "count", affected)
			}

			// Move records that exceeded max attempts to the Dead Letter Queue
			if err := repo.MoveToDLQ(ctx); err != nil {
				logger.Error("Janitor: DLQ maintenance failure", "error", err)
			}

		case <-ctx.Done():
			logger.Info("🛑 Janitor: Stopping maintenance goroutine")
			return
		}
	}
}
