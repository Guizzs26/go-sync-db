package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Guizzs26/go-sync-db/internal/broker"
	"github.com/Guizzs26/go-sync-db/internal/config"
	"github.com/Guizzs26/go-sync-db/internal/db"
	"github.com/Guizzs26/go-sync-db/internal/service"
	"github.com/Guizzs26/go-sync-db/pkg/infra"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	// Configuration & Logger Initialization
	cfg := config.Load()
	logger := infra.SetupLogger(cfg)
	slog.SetDefault(logger)
	defer infra.CloseLogger()

	logger.Info("🔧 Initializing Firebird Collector Service...",
		"unit_id", cfg.UnitID,
	)

	// Setup Graceful Shutdown Context
	// This context will be canceled when SIGINT (Ctrl+C) or SIGTERM (Docker stop) is received
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Connect to Legacy Database (Firebird)
	// We use the specific Unit DB configuration
	fbRepo, err := db.NewFirebirdRepository(cfg.FirebirdURL, logger)
	if err != nil {
		logger.Error("FATAL: Failed to connect to Firebird database", "error", err)
		os.Exit(1)
	}
	defer fbRepo.Close()

	// Connect to Message Broker (RabbitMQ)
	rabbit, err := broker.NewRabbitMQClient(cfg.RabbitMQURL, logger)
	if err != nil {
		logger.Error("FATAL: Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbit.Close()

	// Topology Setup (Infrastructure as Code)
	// We ensure the destination exchange exists before starting the application
	// This prevents runtime errors if the broker is fresh
	logger.Info("⚙️ Verifying Broker Topology...")
	if err := setupTopology(cfg.RabbitMQURL); err != nil {
		logger.Error("FATAL: Failed to declare RabbitMQ topology", "error", err)
		os.Exit(1)
	}

	// Service Initialization (Dependency Injection)
	collectorService := service.NewFBCollectorService(fbRepo, rabbit, cfg.UnitID, logger)

	// Start the Main Loop
	logger.Info("🚀 Collector is running. Polling Firebird for changes...")

	// This is a blocking call. It will run untill 'ctx' is canceled
	collectorService.Run(ctx)

	// Shutdown Sequence
	// If we reached here, the context was canceled. We allow a small buffer for cleanup if needed
	logger.Info("✅ Collector service shut down successfully.")
}

// setupTopology ensures that the necessary Exchanges exist in RabbitMQ
// This is critical because the Collector publishes to an Exchange, not a Queue
func setupTopology(amqpURL string) error {
	// We create a temporary connection just for setup to avoid polluting the main service logic
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	// Declare the Topic Exchange where units will publish their changes
	err = ch.ExchangeDeclare(
		service.ExchangeFBtoHQ, // Name: "pax.fb.to.hq"
		"topic",                // Type: Topic allows routing by "unit.1.sync"
		true,                   // Durable: Yes
		false,                  // Auto-deleted: No
		false,                  // Internal: No
		false,                  // No-wait: No
		nil,                    // Arguments
	)
	if err != nil {
		return err
	}

	return nil
}
