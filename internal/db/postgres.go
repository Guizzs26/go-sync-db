package db

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/Guizzs26/go-sync-db/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewPostgresRepository initializes the connection pool with explicit health checks
func NewPostgresRepository(ctx context.Context, connString string, logger *slog.Logger) (*PostgresRepository, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %v", err)
	}

	p, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %v", err)
	}

	if err := p.Ping(ctx); err != nil {
		return nil, fmt.Errorf("postgres ping failed: %v", err)
	}

	return &PostgresRepository{
		pool:   p,
		logger: logger,
	}, nil
}

// FetchAndClaim picks pending or stale messages and marks them as 'processing' in a single transaction
func (r *PostgresRepository) FetchAndClaim(ctx context.Context, batchSize int) ([]models.OutboxEntry, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	// CTE Strategy:
	// 1. Get new pending messages
	// 2. Get stale messages that were 'processing' for too long (rescue logic)
	query := `
        WITH candidates AS (
            (SELECT id, correlation_id, unit_id, table_name, operation, payload, attempts
             FROM pg_sync_outbox
             WHERE status = 'pending' AND attempts < 5
             ORDER BY created_at ASC LIMIT $1)
            UNION ALL
            (SELECT id, correlation_id, unit_id, table_name, operation, payload, attempts
             FROM pg_sync_outbox
             WHERE status = 'processing' AND updated_at < NOW() - INTERVAL '5 minutes' AND attempts < 5
             ORDER BY updated_at ASC LIMIT $1)
            LIMIT $1
        )
        SELECT * FROM candidates FOR UPDATE SKIP LOCKED`

	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to query outbox candidates: %v", err)
	}
	defer rows.Close()

	var entries []models.OutboxEntry
	var ids []int64

	for rows.Next() {
		var e models.OutboxEntry
		err := rows.Scan(&e.ID, &e.CorrelationID, &e.UnitID, &e.TableName, &e.Operation, &e.Payload, &e.Attempts)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox row: %v", err)
		}
		entries = append(entries, e)
		ids = append(ids, e.ID)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	// Atomic update to prevent other instances from picking the same batch
	if _, err = tx.Exec(ctx, `
        UPDATE pg_sync_outbox 
        SET status = 'processing',
            attempts = attempts + 1,
            updated_at = NOW()
        WHERE id = ANY($1)`, ids,
	); err != nil {
		return nil, fmt.Errorf("failed to claim messages: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %v", err)
	}

	return entries, nil
}

func (r *PostgresRepository) MarkAsSent(ctx context.Context, id int64) error {
	query := `UPDATE pg_sync_outbox SET status = 'sent', updated_at = NOW() WHERE id = $1`
	if _, err := r.pool.Exec(ctx, query, id); err != nil {
		return fmt.Errorf("failed to mark as sent (ID: %v): %v", id, err)
	}
	return nil
}

func (r *PostgresRepository) MarkAsError(ctx context.Context, id int64, errLog string) error {
	// Note: attempts are already incremented in FetchAndClaim to prevent infinite loops
	query := `UPDATE pg_sync_outbox SET status = 'error', error_log = $2, updated_at = NOW() WHERE id = $1`
	if _, err := r.pool.Exec(ctx, query, id, errLog); err != nil {
		return fmt.Errorf("failed to mark as error (ID: %v): %v", id, err)
	}
	return nil
}

func (r *PostgresRepository) MarkAsPending(ctx context.Context, id int64, errLog string) error {
	query := `UPDATE pg_sync_outbox SET status = 'pending', error_log = $2, updated_at = NOW() WHERE id = $1`
	if _, err := r.pool.Exec(ctx, query, id, errLog); err != nil {
		return fmt.Errorf("failed to mark as pending (ID: %v): %v", id, err)
	}
	return nil
}

func (r *PostgresRepository) MoveToDLQ(ctx context.Context) error {
	query := `
        WITH moved AS (
            DELETE FROM pg_sync_outbox
            WHERE attempts >= 5
            RETURNING *
        )
        INSERT INTO pg_sync_dlq (
            correlation_id, unit_id, table_name, operation, 
            payload, attempts, error_log, failed_at
        )
        SELECT 
            correlation_id, unit_id, table_name, operation,
            payload, attempts, error_log, NOW()
        FROM moved`
	if _, err := r.pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to move poison pills to DLQ: %v", err)
	}
	return nil
}

func (r *PostgresRepository) Close() {
	r.pool.Close()
}
