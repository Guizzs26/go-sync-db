package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/models"
)

// Repository defines the contract for outbox data persistence
type Repository interface {
	FetchAndClaim(ctx context.Context, batchSize int) ([]models.OutboxEntry, error)
	MarkAsSent(ctx context.Context, id int64) error
	MarkAsError(ctx context.Context, id int64, errLog string) error
	MarkAsPending(ctx context.Context, id int64, errLog string) error
	MarkManyAsPending(ctx context.Context, ids []int64, reason string) error
}

// BrokerClient defines the contract for message publishing
type BrokerClient interface {
	Publish(ctx context.Context, routingKey string, entry models.OutboxEntry) error
}

// SyncService orchestrates the movement of data between the Database and the Message Broker
type SyncService struct {
	repo   Repository
	broker BrokerClient
	logger *slog.Logger
}

func NewSyncService(r Repository, b BrokerClient, l *slog.Logger) *SyncService {
	return &SyncService{
		repo:   r,
		broker: b,
		logger: l,
	}
}

// ProcessNextBatch captures and sends a batch of messages to the broker
// It features instant responsiveness to shutdown signals and atomic batch recovery
func (s *SyncService) ProcessNextBatch(ctx context.Context, batchSize int) error {
	entries, err := s.repo.FetchAndClaim(ctx, batchSize)
	if err != nil {
		return fmt.Errorf("fetch failure: %v", err)
	}
	if len(entries) == 0 {
		return nil
	}

	for i, e := range entries {
		select {
		case <-ctx.Done():
			s.logger.Warn("Shutdown signal received. Reverting remaining messages in batch.")
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			remainingIDs := s.extractRemainingIDs(entries, i)
			_ = s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "graceful_shutdown_interruption")

			cancel()
			return ctx.Err()
		default:
		}

		l := s.logger.With("correlation_id", e.CorrelationID)
		routingKey := fmt.Sprintf("pax.unit.%d.%s.%s", e.UnitID, e.TableName, e.Operation)

		if err := s.broker.Publish(ctx, routingKey, e); err != nil {
			l.Error("publish failed, aborting batch", "error", err)

			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			remainingIDs := s.extractRemainingIDs(entries, i)
			_ = s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "broker_publish_failure")

			cancel()
			return fmt.Errorf("broker failure: %v", err)
		}

		// DB CHECKPOINT
		if err := s.repo.MarkAsSent(ctx, e.ID); err != nil {
			l.Error("message sent but failed to update status in DB", "error", err)

			// If the bank failed here, the 'e.ID' is already in the broker (imminent duplication)
			// We try to save the REST of the batch (i+1 onwards)
			if i+1 < len(entries) {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				remainingIDs := s.extractRemainingIDs(entries, i+1)
				_ = s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "db_status_update_failure")
				cancel()
			}
			return fmt.Errorf("db checkpoint failure: %v", err)
		}
	}

	return nil
}

func (s *SyncService) extractRemainingIDs(entries []models.OutboxEntry, start int) []int64 {
	ids := make([]int64, 0, len(entries)-start)
	for i := start; i < len(entries); i++ {
		ids = append(ids, entries[i].ID)
	}
	return ids
}
