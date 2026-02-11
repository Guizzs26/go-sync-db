package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/broker"
	"github.com/Guizzs26/go-sync-db/internal/config"
	"github.com/Guizzs26/go-sync-db/internal/db"
	"github.com/Guizzs26/go-sync-db/internal/mapper"
	"github.com/Guizzs26/go-sync-db/internal/processor"
	"github.com/Guizzs26/go-sync-db/pkg/infra"
	_ "github.com/Guizzs26/go-sync-db/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := config.Load()
	logger := infra.SetupLogger(cfg)
	slog.SetDefault(logger)

	unitIDStr := os.Getenv("UNIT_ID")
	if unitIDStr == "" {
		logger.Error("CRITICAL: UNIT_ID environment variable is missing")
		os.Exit(1)
	}
	unitID, err := strconv.Atoi(unitIDStr)
	if err != nil {
		logger.Error("CRITICAL: UNIT_ID must be an integer", "value", unitIDStr)
		os.Exit(1)
	}

	// Graceful Shutdown Context
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("🔥 Consumer initializing...",
		"unit_id", unitID,
		"version", "1.0.0",
	)

	// Initialize Firebird Infrastructure
	repo, err := db.NewFirebirdRepository(cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("CRITICAL: Firebird connection failed", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	// Initialize Core Logic
	handler := processor.NewSyncHandler(repo, mapper.NewSQLBuilder(), logger)

	// Start Observability Server (Port 9091)
	go startObservabilityServer("9091", logger)

	connBackoff := infra.NewBackoff(1*time.Second, 60*time.Second, 2.0)

	for {
		select {
		case <-ctx.Done():
			logger.Info("🛑 Shutdown signal received before connection")
			return
		default:
			consumer, err := broker.NewRabbitMQConsumer(cfg.RabbitMQURL, unitID, handler, logger)
			if err != nil {
				wait := connBackoff.Next()
				logger.Error("RabbitMQ connection failed, retrying...",
					"wait_duration", wait,
					"error", err,
				)

				select {
				case <-ctx.Done():
					return
				case <-time.After(wait):
					continue
				}
			}

			connBackoff.Reset()
			logger.Info("✅ Connected to Broker. Listening for events...")

			if err := consumer.Listen(ctx); err != nil {
				logger.Error("⚠️ Consumer connection lost", "error", err)
			}

			consumer.Close()
		}
	}
}

func startObservabilityServer(port string, logger *slog.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("CONSUMER ALIVE"))
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	logger.Info("📊 Observability server online", "url", "http://localhost:"+port+"/metrics")
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("Observability server failed", "error", err)
	}
}
