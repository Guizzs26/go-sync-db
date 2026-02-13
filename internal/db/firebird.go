package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Guizzs26/go-sync-db/internal/models"
	"github.com/Guizzs26/go-sync-db/pkg/encoding"
	_ "github.com/nakagami/firebirdsql"
)

// FirebirdRepository handles data infrastructure at the branch level
type FirebirdRepository struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewFirebirdRepository initializes a connection pool for Firebird 2.5
func NewFirebirdRepository(connString string, logger *slog.Logger) (*FirebirdRepository, error) {
	db, err := sql.Open("firebirdsql", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to open firebird connection: %v", err)
	}

	// Connection pool settings optimized for legacy systems
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(10 * time.Minute)

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("firebird ping failed: %v", err)
	}

	logger.Info("Connected to Firebird successfully", "dialect", 3)

	return &FirebirdRepository{
		db:     db,
		logger: logger,
	}, nil
}

// GetNextID emulates the Delphi application protocol by incrementing the INDICE table
// This operation must be executed within the same transaction as the main insertion
func (r *FirebirdRepository) GetNextID(ctx context.Context, tx *sql.Tx, generatorName string) (int, error) {
	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	updateQuery := `UPDATE INDICE SET VALOR = VALOR + 1 WHERE NOME = ?`
	res, err := tx.ExecContext(opCtx, updateQuery, generatorName)
	if err != nil {
		return 0, fmt.Errorf("failed to increment index %s: %v", generatorName, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return 0, fmt.Errorf("generator name '%s' not found in INDICE table", generatorName)
	}

	var nextID int
	selectQuery := `SELECT VALOR FROM INDICE WHERE NOME = ?`
	err = tx.QueryRowContext(opCtx, selectQuery, generatorName).Scan(&nextID)
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve updated index %s: %v", generatorName, err)
	}

	r.logger.Debug("Generated new ID", "generator", generatorName, "id", nextID)
	return nextID, nil
}

// IsProcessed checks if a correlation_id has already been synchronized
// This is the core mechanism for absolute idempotency
func (r *FirebirdRepository) IsProcessed(ctx context.Context, correlationID string) (bool, error) {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `SELECT FIRST 1 1 FROM SYNC_CONTROL WHERE CORRELATION_ID = ?`

	var exists bool
	err := r.db.QueryRowContext(opCtx, query, correlationID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check idempotency: %v", err)
	}

	return true, nil
}

// MarkAsProcessed records the correlation_id in the SYNC_CONTROL table
func (r *FirebirdRepository) MarkAsProcessed(ctx context.Context, tx *sql.Tx, correlationID string) error {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `INSERT INTO SYNC_CONTROL (CORRELATION_ID) VALUES (?)`

	_, err := tx.ExecContext(opCtx, query, correlationID)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "violation") || strings.Contains(msg, "unique") || strings.Contains(msg, "primary") {
			r.logger.Warn("Idempotency race detected: correlation_id already exists in DB", "id", correlationID)
			return nil
		}

		return fmt.Errorf("failed to mark message as processed: %v", err)
	}
	return nil
}

// BeginTx starts a transaction with ReadCommitted isolation level
func (r *FirebirdRepository) BeginTx(ctx context.Context) (*sql.Tx, error) {
	return r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
}

// Close gracefully shuts down the database connection pool
func (r *FirebirdRepository) Close() error {
	r.logger.Info("Closing Firebird connection pool")
	return r.db.Close()
}

// ---------- FB -> PG ---------- //

// FetchOutboxPending retrieves the oldest N records from the local sync queue
func (r *FirebirdRepository) FetchOutboxPending(ctx context.Context, limit int) ([]models.FBOutboxRecord, error) {
	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
		SELECT FIRST ? ID, TABLE_NAME, OP_TYPE, PK_VALUE, CREATED_AT 
		FROM FB_SYNC_OUTBOX 
		ORDER BY ID ASC`

	rows, err := r.db.QueryContext(opCtx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query outbox: %w", err)
	}
	defer rows.Close()

	var records []models.FBOutboxRecord
	for rows.Next() {
		var rec models.FBOutboxRecord
		if err := rows.Scan(&rec.ID, &rec.TableName, &rec.OpType, &rec.PKValue, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan error in outbox: %w", err)
		}
		records = append(records, rec)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return records, nil
}

// FetchFullRecord retrieves the actual data row from the source table dynamically
// It handles column discovery and WIN1252 -> UTF8 conversion automatically
func (r *FirebirdRepository) FetchFullRecord(ctx context.Context, tableName string, pkValue string) (map[string]any, error) {
	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Resolve Primary Key Column Name (Security & Mapping)
	pkCol := resolvePKColumn(tableName)
	if pkCol == "" {
		return nil, fmt.Errorf("unknown primary key for table %s", tableName)
	}

	// Dynamic Query Construction (Safe due to whitelist in resolvePKColumn)
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s = ?", tableName, pkCol)

	rows, err := r.db.QueryContext(opCtx, query, pkValue)
	if err != nil {
		return nil, fmt.Errorf("failed to query full record: %w", err)
	}
	defer rows.Close()

	// Dynamic Column Scanning
	// Since we don't know the schema at compile time, we must verify columns dynamically
	if !rows.Next() {
		return nil, sql.ErrNoRows // Record might have been physically deleted
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	// Create a slice of any to hold pointers to values
	values := make([]any, len(columns))
	valuePtrs := make([]any, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	if err := rows.Scan(valuePtrs...); err != nil {
		return nil, err
	}

	// 4. Map Construction and Charset Conversion
	result := make(map[string]any)
	for i, colName := range columns {
		val := values[i]

		// Firebird often returns []byte for strings. We must decode WIN1252
		switch v := val.(type) {
		case []byte:
			result[colName] = encoding.ToUTF8(v)
		case nil:
			result[colName] = nil
		default:
			result[colName] = v
		}
	}

	return result, nil
}

// DeleteOutbox removes the log entry after successful publishing
func (r *FirebirdRepository) DeleteOutbox(ctx context.Context, id int64) error {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `DELETE FROM FB_SYNC_OUTBOX WHERE ID = ?`
	_, err := r.db.ExecContext(opCtx, query, id)
	return err
}

// resolvePKColumn maps table names to their Legacy Primary Key column names
// This prevents SQL injection and handles inconsistent naming
func resolvePKColumn(tableName string) string {
	switch strings.ToUpper(tableName) {
	case "CLIENTE":
		return "IDCLIENTE"
	case "CONTATO":
		return "ID"
	case "PARCELAS_CLIENTE":
		return "PARCELA_ID"
	default:
		return ""
	}
}
