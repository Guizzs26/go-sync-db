package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/models"
)

const MaxBatchMemoryThresholdMB = 20

// Repository defines the contract for outbox data persistence
type Repository interface {
	FetchAndClaim(ctx context.Context, batchSize int) ([]models.OutboxEntry, error)
	MarkAsSent(ctx context.Context, id int64) error
	MarkAsError(ctx context.Context, id int64, errLog string) error
	MarkManyAsPending(ctx context.Context, ids []int64, note string, strategy models.RevertStrategy) error
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
	start := time.Now()

	entries, err := s.repo.FetchAndClaim(ctx, batchSize)
	if err != nil {
		return fmt.Errorf("fetch failure: %v", err)
	}
	if len(entries) == 0 {
		return nil
	}

	// Monitor batch weight to prevent OOM
	var batchBytes int
	for _, e := range entries {
		batchBytes += e.EstimateBytes()
	}
	batchMB := batchBytes / (1024 * 1024)
	if batchMB > MaxBatchMemoryThresholdMB {
		// Log as Warn: This is a signal to reduce BATCH_SIZE in .env (if necessary)
		s.logger.Warn("Heavy batch detected: memory pressure risk",
			"size_mb", batchMB,
			"threshold_mb", MaxBatchMemoryThresholdMB,
			"count", len(entries),
		)
	}

	for i, e := range entries {
		// Instant responsiveness to shutdown signals
		select {
		case <-ctx.Done():
			s.logger.Warn("Shutdown signal received. Reverting remaining messages.")
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			remainingIDs := s.extractRemainingIDs(entries, i)
			_ = s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "graceful_shutdown", models.StrategyInfraFailure)
			cancel()
			return ctx.Err()
		default:
		}

		l := s.logger.With("correlation_id", e.CorrelationID)

		// Whitelist validation
		// Normalizes table name and checks against the Registry
		cleanTable := strings.ToLower(strings.TrimSpace(e.TableName))
		_, allowed := models.TableRegistry[strings.ToUpper(cleanTable)]

		if !allowed || !isValidOperation(e.Operation) {
			l.Error("Security Trigger: invalid metadata. Moving to error state.", "table", e.TableName)
			_ = s.repo.MarkAsError(ctx, e.ID, "security_violation: invalid table or operation")
			continue
		}

		// Secure Routing Key construction using sanitized data
		routingKey := fmt.Sprintf("pax.unit.%d.%s.%s", e.UnitID, cleanTable, e.Operation)

		// Broker Publication (Infra Failure Path)
		if err := s.broker.Publish(ctx, routingKey, e); err != nil {
			l.Error("publish failed, aborting batch", "error", err)

			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			remainingIDs := s.extractRemainingIDs(entries, i)
			// Refund attempt (attempts-1) since the broker is down
			_ = s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "broker_offline", models.StrategyInfraFailure)
			cancel()
			return fmt.Errorf("broker failure: %v", err)
		}

		// Final DB Checkpoint (Business Failure Path)
		if err := s.repo.MarkAsSent(ctx, e.ID); err != nil {
			l.Error("message sent but failed to update status in DB", "error", err)

			if i+1 < len(entries) {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				remainingIDs := s.extractRemainingIDs(entries, i+1)
				// Keep attempt count to avoid infinite duplication loops on data errors
				_ = s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "db_checkpoint_failure", models.StrategyBusinessFailure)
				cancel()
			}
			return fmt.Errorf("db checkpoint failure: %v", err)
		}
	}

	defer func() {
		if len(entries) > 0 {
			s.logger.Info("Batch cycle telemetry",
				"count", len(entries),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}
	}()

	return nil
}

func (s *SyncService) extractRemainingIDs(entries []models.OutboxEntry, start int) []int64 {
	ids := make([]int64, 0, len(entries)-start)
	for i := start; i < len(entries); i++ {
		ids = append(ids, entries[i].ID)
	}
	return ids
}

func isValidOperation(op string) bool {
	return op == "I" || op == "U" || op == "D"
}
