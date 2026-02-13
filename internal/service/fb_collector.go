package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/models"
	"github.com/google/uuid"
)

const (
	ExchangeFBtoHQ = "pax.fb.to.hq"
	BatchSize      = 50
)

// CollectorRepository defines the data access contract for the Collector
type CollectorRepository interface {
	FetchOutboxPending(ctx context.Context, limit int) ([]models.FBOutboxRecord, error)
	FetchFullRecord(ctx context.Context, tableName string, pkValue string) (map[string]any, error)
	DeleteOutbox(ctx context.Context, id int64) error
}

// MessageBroker defines the publishing contract
type MessageBroker interface {
	PublishToExchange(ctx context.Context, exchange, routingKey string, payload any) error
	IsHealthy() bool
}

// FBCollectorService orchestrates the extraction of data from Legacy Firebird to RabbitMQ
type FBCollectorService struct {
	repo   CollectorRepository
	broker MessageBroker
	logger *slog.Logger
	unitID int
}

// NewFBCollectorService creates a new instance of the collector service
func NewFBCollectorService(repo CollectorRepository, broker MessageBroker, unitID int, logger *slog.Logger) *FBCollectorService {
	return &FBCollectorService{
		repo:   repo,
		broker: broker,
		logger: logger,
		unitID: unitID,
	}
}

// Run starts the polling loop. It blocks until the context is canceled
func (s *FBCollectorService) Run(ctx context.Context) {
	// Polling interval: 1 second. Firebird triggers are near real-time,
	// but we don't want to hammer the legacy DB.
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	s.logger.Info("🔥 Firebird Collector Service started", "unit_id", s.unitID)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Collector shutting down...")
			return
		case <-ticker.C:
			// Check broker health before attempting to read DB
			if !s.broker.IsHealthy() {
				s.logger.Warn("Broker is offline, skipping collection cycle")
				continue
			}

			if err := s.processBatch(ctx); err != nil {
				s.logger.Error("Collector batch cycle failed", "error", err)
			}
		}
	}
}

// processBatch handles the extraction, transformation, and loading (ETL) pipeline
func (s *FBCollectorService) processBatch(ctx context.Context) error {
	// Fetch pending logs from the Outbox
	records, err := s.repo.FetchOutboxPending(ctx, BatchSize)
	if err != nil {
		return fmt.Errorf("failed to fetch outbox: %w", err)
	}

	if len(records) == 0 {
		return nil // Idle cycle
	}

	s.logger.Debug("Processing outbox batch", "count", len(records))

	// Process each record sequentially (FIFO is critical for consistency)
	for _, rec := range records {
		if err := s.processSingleRecord(ctx, rec); err != nil {
			// If one fails, we stop the entire batch to preserve order.
			// The next tick will retry this same record.
			return fmt.Errorf("failed to process record ID %d: %w", rec.ID, err)
		}
	}

	return nil
}

func (s *FBCollectorService) processSingleRecord(ctx context.Context, rec models.FBOutboxRecord) error {
	payload := models.FBEventPayload{
		EventID:   uuid.NewString(),
		UnitID:    s.unitID,
		TableName: rec.TableName,
		Operation: rec.OpType,
		PKValue:   rec.PKValue,
		Timestamp: time.Now(),
		Data:      nil,
	}

	// Hydrate Data (Snapshot)
	// If it's a DELETE operation, we don't need the body, just the PK
	if rec.OpType != "D" {
		data, err := s.repo.FetchFullRecord(ctx, rec.TableName, rec.PKValue)
		if err != nil {
			// CRITICAL: Handling "Ghost Records"
			// If the record exists in Outbox but was physically deleted from the source table
			// before we could read it, we have a consistency gap
			if errors.Is(err, sql.ErrNoRows) {
				s.logger.Warn("Record missing in source table (Ghost Record). Skipping.",
					"table", rec.TableName, "pk", rec.PKValue)

				// We delete from Outbox to unblock the queue, effectively ignoring this event
				// Alternatively, we could convert it to a DELETE event if business logic requires
				return s.repo.DeleteOutbox(ctx, rec.ID)
			}
			return fmt.Errorf("failed to fetch snapshot: %w", err)
		}
		payload.Data = data
	}

	// Publish to RabbitMQ
	// We use a specific routing key: unit.{ID}.sync
	// This allows the HQ to bind queues specifically for this unit or use a wildcard (*.sync)
	routingKey := fmt.Sprintf("unit.%d.sync", s.unitID)

	err := s.broker.PublishToExchange(ctx, ExchangeFBtoHQ, routingKey, payload)
	if err != nil {
		return fmt.Errorf("broker publish failed: %w", err)
	}

	// Commit: Remove from Outbox
	// This ensures At-Least-Once delivery. If this fails, we will resend the event
	// The HQ Consumer must implement Idempotency to handle duplicates
	if err := s.repo.DeleteOutbox(ctx, rec.ID); err != nil {
		return fmt.Errorf("failed to delete outbox entry: %w", err)
	}

	return nil
}
