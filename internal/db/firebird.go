package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

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

	if err := db.Ping(); err != nil {
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
	updateQuery := `UPDATE INDICE SET VALOR = VALOR + 1 WHERE NOME = ?`
	res, err := tx.ExecContext(ctx, updateQuery, generatorName)
	if err != nil {
		return 0, fmt.Errorf("failed to increment index %s: %v", generatorName, err)
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		return 0, fmt.Errorf("generator name '%s' not found in INDICE table", generatorName)
	}

	var nextID int
	selectQuery := `SELECT VALOR FROM INDICE WHERE NOME = ?`
	err = tx.QueryRowContext(ctx, selectQuery, generatorName).Scan(&nextID)
	if err != nil {
		return 0, fmt.Errorf("failed to retrieve updated index %s: %v", generatorName, err)
	}

	r.logger.Debug("Generated new ID", "generator", generatorName, "id", nextID)
	return nextID, nil
}

// IsProcessed checks if a correlation_id has already been synchronized
// This is the core mechanism for absolute idempotency
func (r *FirebirdRepository) IsProcessed(ctx context.Context, correlationID string) (bool, error) {
	query := `SELECT FIRST 1 1 FROM SYNC_CONTROL WHERE CORRELATION_ID = ?`

	var exists bool
	err := r.db.QueryRowContext(ctx, query, correlationID).Scan(&exists)
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
	query := `INSERT INTO SYNC_CONTROL (CORRELATION_ID) VALUES (?)`

	_, err := tx.ExecContext(ctx, query, correlationID)
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
