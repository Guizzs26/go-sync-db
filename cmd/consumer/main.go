package main

import (
	"context"
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
	"github.com/Guizzs26/go-sync-db/internal/service"
	"github.com/Guizzs26/go-sync-db/pkg/infra"
)

func main() {
	cfg := config.Load()
	logger := infra.SetupLogger(cfg)

	unitID, _ := strconv.Atoi(os.Getenv("UNIT_ID"))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Initialize Infrastructure
	repo, err := db.NewFirebirdRepository(cfg.DatabaseURL, logger)
	if err != nil {
		logger.Error("critical: firebird connection failed", "error", err)
		os.Exit(1)
	}
	defer repo.Close()

	handler := processor.NewSyncHandler(repo, mapper.NewSQLBuilder(), logger)

	// Reconnection Strategy using Exponential Backoff
	// Min: 1s, Max: 60s, Factor: 2
	b := service.NewBackoff(1, 60, 2)

	logger.Info("Sentinel Consumer initialized", "unit_id", unitID)

	for {
		consumer, err := broker.NewRabbitMQConsumer(cfg.RabbitMQURL, unitID, handler, logger)
		if err != nil {
			wait := b.Next()
			logger.Error("connection failed, retrying", "wait", wait, "error", err)

			select {
			case <-ctx.Done():
				return
			case <-time.After(wait):
				continue
			}
		}

		// Reset backoff upon successful connection
		b.Reset()
		logger.Info("connected to broker, starting listener")

		// Listen is a blocking call. It returns if the channel/connection closes.
		if err := consumer.Listen(ctx); err != nil {
			logger.Error("consumer connection lost", "error", err)
		}

		consumer.Close()

		select {
		case <-ctx.Done():
			logger.Info("graceful shutdown complete")
			return
		default:
			// Loop continues and attempts reconnection
			continue
		}
	}
}
