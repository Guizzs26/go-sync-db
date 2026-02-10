package service

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Guizzs26/go-sync-db/internal/models"
)

// Repository defines the contract for outbox data persistence
type Repository interface {
	FetchAndClaim(ctx context.Context, batchSize int) ([]models.OutboxEntry, error)
	MarkAsSent(ctx context.Context, id int64) error
	MarkAsError(ctx context.Context, id int64, errLog string) error
	MarkAsPending(ctx context.Context, id int64, errLog string) error
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

// ProcessNextBatch fetches a group of pending messages and attempts to publish them
func (s *SyncService) ProcessNextBatch(ctx context.Context, batchSize int) error {
	entries, err := s.repo.FetchAndClaim(ctx, batchSize)
	if err != nil {
		s.logger.Error("failed to fetch pending entries from database", "error", err)
		return fmt.Errorf("error fetching pending entries: %v", err)
	}

	if len(entries) == 0 {
		return nil
	}

	s.logger.Info("batch captured for processing", "count", len(entries))

	for _, e := range entries {
		l := s.logger.With(
			"correlation_id", e.CorrelationID,
			"unit_id", e.UnitID,
			"table", e.TableName,
		)

		l.Debug("processing message")

		// Dynamic routing key following the pattern: pax.unit.<id>.<table_name>.<operation>
		routingKey := fmt.Sprintf("pax.unit.%d.%s.%s",
			e.UnitID,
			e.TableName,
			e.Operation,
		)

		if err := s.broker.Publish(ctx, routingKey, e); err != nil {
			l.Error("failed to publish to RabbitMQ", "error", err)
			// Return to PENDING so the next poll (or the rescue logic) picks it up
			s.repo.MarkAsPending(ctx, e.ID, err.Error())
			continue
		}

		// Finalize the state machine in the database
		if err := s.repo.MarkAsSent(ctx, e.ID); err != nil {
			l.Warn("message published but failed to mark as 'sent' in database", "error", err)
		} else {
			l.Info("synchronization completed successfully")
		}
	}

	return nil
}
