package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/db"
	"github.com/Guizzs26/go-sync-db/internal/mapper"
	"github.com/Guizzs26/go-sync-db/internal/models"
	"github.com/Guizzs26/go-sync-db/pkg/metrics"
)

// SyncHandler orchestrates the consumption and persistence of synchronization messages
type SyncHandler struct {
	repo   *db.FirebirdRepository
	mapper *mapper.SQLBuilder
	logger *slog.Logger
}

// NewSyncHandler creates a new instance of the synchronization orchestrator
func NewSyncHandler(repo *db.FirebirdRepository, mapper *mapper.SQLBuilder, logger *slog.Logger) *SyncHandler {
	return &SyncHandler{
		repo:   repo,
		mapper: mapper,
		logger: logger,
	}
}

// ProcessMessage executes the complete synchronization cycle with internal retry and dynamic timeouts
func (h *SyncHandler) ProcessMessage(ctx context.Context, entry models.OutboxEntry) (err error) {
	start := time.Now()
	tableName := strings.ToUpper(entry.TableName)

	defer func() {
		duration := time.Since(start).Seconds()
		status := "success"

		if err != nil {
			if strings.HasPrefix(err.Error(), "FATAL:") {
				status = "fatal_error"
			} else {
				status = "transient_error"
			}
		}

		metrics.ConsumerDuration.WithLabelValues(
			status,
			tableName,
			entry.Operation,
		).Observe(duration)
	}()

	l := h.logger.With(
		"correlation_id", entry.CorrelationID,
		"table", tableName,
		"operation", entry.Operation,
	)

	// Whitelist & Metadata Validation
	pkColumn, allowed := models.TableRegistry[tableName]
	if !allowed {
		l.Error("Fatal: table not allowed in whitelist", "table", tableName)
		return fmt.Errorf("FATAL: table %s is not whitelisted", tableName)
	}

	// Parse Payload
	var payload map[string]any
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		l.Error("Fatal: failed to parse payload", "error", err)
		return fmt.Errorf("FATAL: payload unmarshal error: %v", err)
	}

	// Idempotency Check (Fast check, short timeout)
	// Checkamos antes de tentar qualquer lock no banco
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	alreadyProcessed, err := h.repo.IsProcessed(checkCtx, entry.CorrelationID)
	cancel()

	if err != nil {
		return fmt.Errorf("idempotency check failed: %v", err)
	}
	if alreadyProcessed {
		l.Info("Message already processed, skipping to ACK")
		return nil
	}

	// Transaction Retry Loop
	const maxRetries = 3
	var lastErr error

	// INSERTs are append-only and fast. UPDATEs involve index scans/FK checks and are slower
	opTimeout := 10 * time.Second
	if entry.Operation == "U" {
		opTimeout = 15 * time.Second
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		// Create a fresh context for each attempt with the dynamic duration
		txCtx, txCancel := context.WithTimeout(ctx, opTimeout)
		err = h.executeTransaction(txCtx, entry, payload, pkColumn, tableName)
		txCancel()

		if err == nil {
			l.Info("Successfully synchronized to Firebird")
			return nil
		}

		// Detect Deadlock/Lock Conflict
		if h.isDeadlock(err) {
			lastErr = err

			// Increment lock retry counter
			metrics.ConsumerRetries.WithLabelValues(tableName).Inc()

			// Internal linear backoff for locks (quick retry strategy)
			// Attempt 1: 200ms, Attempt 2: 400ms, Attempt 3: 600ms
			backoff := time.Duration(attempt) * 200 * time.Millisecond

			l.Warn("Firebird lock contention detected, retrying internally",
				"attempt", attempt,
				"backoff", backoff,
				"error", err,
			)

			time.Sleep(backoff)
			continue
		}

		// Non-recoverable error (Syntax, Constraint Violation, etc)
		// Fail fast without retrying
		return err
	}

	return fmt.Errorf("failed after %d attempts (last error: %v)", maxRetries, lastErr)
}

// executeTransaction encapsulates the atomic write operation
func (h *SyncHandler) executeTransaction(ctx context.Context, entry models.OutboxEntry, payload map[string]any, pkColumn, tableName string) error {
	tx, err := h.repo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	// Safety: Rollback is a no-op if Commit was already called
	defer tx.Rollback()

	// ID Generation
	if entry.Operation == "I" {
		generatorName := fmt.Sprintf("GEN_%s_ID", tableName)
		nextID, err := h.repo.GetNextID(ctx, tx, generatorName)
		if err != nil {
			if strings.Contains(strings.ToUpper(err.Error()), "INDICE") {
				return fmt.Errorf("FATAL: missing index entry for %s", generatorName)
			}
			return fmt.Errorf("id generation failed: %v", err)
		}
		payload[pkColumn] = nextID
	}

	// Build SQL
	query, args, err := h.buildSQL(entry, payload, pkColumn)
	if err != nil {
		return fmt.Errorf("FATAL: sql build failed: %v", err)
	}

	// Execute SQL
	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("execution error: %v", err)
	}

	// Mark as Processed
	if err := h.repo.MarkAsProcessed(ctx, tx, entry.CorrelationID); err != nil {
		return fmt.Errorf("failed to mark sync control: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %v", err)
	}

	return nil
}

// buildSQL acts as a bridge between the handler logic and the SQL generator
func (h *SyncHandler) buildSQL(entry models.OutboxEntry, payload map[string]any, pkColumn string) (string, []any, error) {
	switch entry.Operation {
	case "I":
		return h.mapper.BuildInsert(entry.TableName, payload)
	case "U":
		pkValue := payload[pkColumn]
		if pkValue == nil {
			return "", nil, fmt.Errorf("primary key value missing in payload for update")
		}
		return h.mapper.BuildUpdate(entry.TableName, pkColumn, pkValue, payload)
	default:
		return "", nil, fmt.Errorf("unsupported operation: %s", entry.Operation)
	}
}

// isDeadlock detects common Firebird concurrency errors
func (h *SyncHandler) isDeadlock(err error) bool {
	msg := strings.ToLower(err.Error())
	// Firebird Error Codes/Messages for Locking:
	// - deadlock
	// - lock conflict
	// - update conflicts with concurrent update
	// - 335544336 (ISC Error Code for deadlock)
	return strings.Contains(msg, "deadlock") ||
		strings.Contains(msg, "lock conflict") ||
		strings.Contains(msg, "concurrent update") ||
		strings.Contains(msg, "335544336")
}
