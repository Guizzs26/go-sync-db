package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/models"
	"github.com/Guizzs26/go-sync-db/pkg/metrics"
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
		return fmt.Errorf("fetch failure: %w", err)
	}
	if len(entries) == 0 {
		return nil
	}

	// Observe actual batch size
	metrics.BatchSize.Observe(float64(len(entries)))

	defer func() {
		// Observe total batch processing duration
		metrics.BatchDuration.Observe(time.Since(start).Seconds())

		if len(entries) > 0 {
			s.logger.Info("Batch cycle telemetry",
				"count", len(entries),
				"duration_ms", time.Since(start).Milliseconds(),
			)
		}
	}()

	var batchBytes int
	for _, e := range entries {
		batchBytes += e.EstimateBytes()
	}
	if batchMB := batchBytes / (1024 * 1024); batchMB > MaxBatchMemoryThresholdMB {
		s.logger.Warn("Heavy batch detected: memory pressure risk",
			"size_mb", batchMB,
			"threshold_mb", MaxBatchMemoryThresholdMB,
			"count", len(entries),
		)
	}

	for i, e := range entries {
		select {
		case <-ctx.Done():
			s.logger.Warn("Shutdown signal received. Reverting remaining messages.")
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			remainingIDs := s.extractRemainingIDs(entries, i)

			if err := s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "graceful_shutdown", models.StrategyInfraFailure); err != nil {
				s.logger.Error("CRITICAL: Failed to revert messages during shutdown", "error", err, "count", len(remainingIDs))
			}
			cancel()
			return ctx.Err()
		default:
		}

		l := s.logger.With("correlation_id", e.CorrelationID)
		cleanTable := strings.ToLower(strings.TrimSpace(e.TableName))
		unitStr := fmt.Sprintf("%d", e.UnitID)

		// Application Firewall: Whitelist & Metadata Validation
		_, allowed := models.TableRegistry[strings.ToUpper(cleanTable)]
		if !allowed || !isValidOperation(e.Operation) {
			l.Error("Security Trigger: invalid metadata", "table", e.TableName)
			_ = s.repo.MarkAsError(ctx, e.ID, "security_violation")

			// Record security/metadata error
			metrics.MessagesProcessed.WithLabelValues("error", unitStr, cleanTable).Inc()
			continue
		}

		routingKey := fmt.Sprintf("pax.unit.%d.%s.%s", e.UnitID, cleanTable, e.Operation)

		// Transport: Publish to Broker
		if err := s.broker.Publish(ctx, routingKey, e); err != nil {
			l.Error("Broker publish failed, aborting batch", "error", err)
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			remainingIDs := s.extractRemainingIDs(entries, i)

			if err := s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "broker_offline", models.StrategyInfraFailure); err != nil {
				s.logger.Error("CRITICAL: Failed to revert messages after broker failure", "error", err)
			}
			cancel()

			// Record infra error
			metrics.MessagesProcessed.WithLabelValues("error", unitStr, cleanTable).Inc()
			return fmt.Errorf("broker failure: %w", err)
		}

		// Final DB Checkpoint
		if err := s.repo.MarkAsSent(ctx, e.ID); err != nil {
			l.Error("Message sent but failed to update status in DB", "error", err)
			if i+1 < len(entries) {
				cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				remainingIDs := s.extractRemainingIDs(entries, i+1)
				_ = s.repo.MarkManyAsPending(cleanupCtx, remainingIDs, "db_checkpoint_failure", models.StrategyBusinessFailure)
				cancel()
			}

			metrics.MessagesProcessed.WithLabelValues("error", unitStr, cleanTable).Inc()
			return fmt.Errorf("db checkpoint failure: %w", err)
		}

		metrics.MessagesProcessed.WithLabelValues("sent", unitStr, cleanTable).Inc()
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

func isValidOperation(op string) bool {
	return op == "I" || op == "U" || op == "D"
}
