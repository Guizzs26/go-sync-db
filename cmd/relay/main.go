package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/broker"
	"github.com/Guizzs26/go-sync-db/internal/config"
	"github.com/Guizzs26/go-sync-db/internal/db"
	"github.com/Guizzs26/go-sync-db/internal/service"
	"github.com/Guizzs26/go-sync-db/pkg/infra"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// Core initialization
	cfg := config.Load()
	logger := infra.SetupLogger(cfg)
	slog.SetDefault(logger)
	defer infra.CloseLogger()

	// Setup graceful shutdown context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Database connection (Postgres Source)
	postgres, err := db.NewPostgresRepository(ctx, cfg.DatabaseURL, logger)
	if err != nil {
		slog.Error("Fatal: failed to connect to Postgres", "error", err)
		os.Exit(1)
	}
	defer postgres.Close()

	// State tracking for Observability
	var currentRabbit *broker.RabbitMQClient

	// Start Observability Server in a background goroutine
	// It uses a closure to always check the most recent RabbitMQ instance
	go startObservabilityServer("9090", postgres, func() bool {
		return currentRabbit != nil && currentRabbit.IsHealthy()
	})

	// Background Maintenance (Janitor)
	maintenanceDone := make(chan struct{})
	go runMaintenance(ctx, postgres, cfg.MaintenanceInterval, logger, maintenanceDone)

	slog.Info("🚀 Sentinel Relay Service started", "pid", os.Getpid(), "batch_size", cfg.BatchSize)

	// Execution Loop
	// We pass the address of currentRabbit so the loop can update the reference
	runMainLoop(ctx, postgres, cfg, logger, &currentRabbit, maintenanceDone)
}

// runMainLoop handles the synchronization lifecycle and infrastructure resilience
func runMainLoop(ctx context.Context, repo *db.PostgresRepository, cfg *config.Config, logger *slog.Logger, rabbitPtr **broker.RabbitMQClient, maintenanceDone chan struct{}) {
	backoff := service.NewBackoff(1*time.Second, 60*time.Second, 2.0)
	var syncService *service.SyncService

	for {
		select {
		case <-ctx.Done():
			logger.Info("👋 Graceful shutdown: stopping main loop")
			if *rabbitPtr != nil {
				(*rabbitPtr).Close()
			}
			<-maintenanceDone
			logger.Info("✅ Sentinel Relay reached safe state")
			return

		default:
			// A. Infrastructure Health Check
			rabbitmq := *rabbitPtr
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
				*rabbitPtr = newRabbit
				backoff.Reset()
				syncService = service.NewSyncService(repo, newRabbit, logger)
			}

			// B. Batch Processing
			if err := syncService.ProcessNextBatch(ctx, cfg.BatchSize); err != nil {
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

			// C. Success Path
			backoff.Reset()
			select {
			case <-time.After(cfg.PollInterval):
			case <-ctx.Done():
			}
		}
	}
}

// runMaintenance manages stale messages and moves poison pills to the DLQ
func runMaintenance(ctx context.Context, repo *db.PostgresRepository, interval time.Duration, logger *slog.Logger, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			logger.Info("🧹 Janitor: Starting structural health checks")
			if affected, err := repo.ResetStaleMessages(ctx, 10); err == nil && affected > 0 {
				logger.Warn("Janitor: Rescued stuck messages", "count", affected)
			}
			if err := repo.MoveToDLQ(ctx); err != nil {
				logger.Error("Janitor: DLQ maintenance failure", "error", err)
			}
		case <-ctx.Done():
			logger.Info("🛑 Janitor: Stopping maintenance goroutine")
			return
		}
	}
}

// startObservabilityServer unifies metrics, health, and readiness probes
func startObservabilityServer(port string, repo *db.PostgresRepository, rabbitHealthy func() bool) {
	mux := http.NewServeMux()

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := repo.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ALIVE"))
	})

	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if rabbitHealthy() {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("READY"))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	slog.Info("📊 Observability server online", "port", port)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("Observability server failed", "error", err)
	}
}
