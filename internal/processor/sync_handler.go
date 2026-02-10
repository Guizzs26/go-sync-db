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

// ProcessMessage executes the complete synchronization cycle for a single message
// It ensures transactional integrity, security via whitelisting, and absolute idempotency
func (h *SyncHandler) ProcessMessage(ctx context.Context, entry models.OutboxEntry) error {
	l := h.logger.With(
		"correlation_id", entry.CorrelationID,
		"table", entry.TableName,
		"operation", entry.Operation,
	)

	// Whitelist & PK Validation
	tableName := strings.ToUpper(entry.TableName)
	pkColumn, allowed := models.TableRegistry[tableName]
	if !allowed {
		l.Error("Fatal: table not allowed in whitelist", "table", tableName)
		// Returning a formatted error with FATAL prefix to help the worker decide not to requeue
		return fmt.Errorf("FATAL: table %s is not whitelisted", tableName)
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Idempotency Check
	// Verify if this message was already successfully applied to this branch
	alreadyProcessed, err := h.repo.IsProcessed(ctx, entry.CorrelationID)
	if err != nil {
		return fmt.Errorf("idempotency check failed: %v", err)
	}
	if alreadyProcessed {
		l.Info("Message already processed, skipping to ACK")
		return nil
	}

	tx, err := h.repo.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %v", err)
	}
	defer tx.Rollback()

	var payload map[string]any
	if err := json.Unmarshal(entry.Payload, &payload); err != nil {
		l.Error("Fatal: failed to parse payload", "error", err)
		return fmt.Errorf("FATAL: payload unmarshal error: %v", err)
	}

	// ID Generation (The Delphi Protocol)
	// If it's an Insert, we manage the legacy INDICE table within the same transaction
	if entry.Operation == "I" {
		generatorName := fmt.Sprintf("GEN_%s_ID", tableName)
		nextID, err := h.repo.GetNextID(ctx, tx, generatorName)
		if err != nil {
			// Check if it's a structural error (missing entry in INDICE table)
			if strings.Contains(strings.ToUpper(err.Error()), "INDICE") {
				l.Error("Fatal: generator not found in INDICE table", "generator", generatorName)
				return fmt.Errorf("FATAL: missing index entry for %s", generatorName)
			}
			return fmt.Errorf("failed to generate ID: %v", err)
		}
		// Inject the newly generated ID into the payload
		payload[pkColumn] = nextID
	}

	// Build and Execute SQL
	query, args, err := h.buildSQL(entry, payload, pkColumn)
	if err != nil {
		return fmt.Errorf("failed to build SQL: %v", err)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		l.Error("SQL execution failed", "query", query, "error", err)
		return fmt.Errorf("execution error: %v", err)
	}

	// Finalize Synchronization Control
	// Mark as processed BEFORE commit to ensure atomicity
	if err := h.repo.MarkAsProcessed(ctx, tx, entry.CorrelationID); err != nil {
		return fmt.Errorf("failed to record sync control: %v", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	l.Info("Successfully synchronized to Firebird")
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

// getPKColumn returns the Primary Key name based on Pax's table naming convention
func (h *SyncHandler) getPKColumn(tableName string) string {
	// Example: CLIENTE -> IDCLIENTE, CONTATO -> ID
	if strings.ToUpper(tableName) == "CLIENTE" {
		return "IDCLIENTE"
	}
	return "ID"
}
