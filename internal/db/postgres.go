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

// FetchAndClaim captures a batch of messages and marks them as 'processing' atomically.
// This implementation uses a single CTE for picking and updating, preventing duplicates
// and ensuring the highest possible throughput for the Outbox pattern.
func (r *PostgresRepository) FetchAndClaim(ctx context.Context, batchSize int) ([]models.OutboxEntry, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start fetch transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Query explanation:
	// 1. The 'target_ids' CTE selects IDs using FOR UPDATE SKIP LOCKED to ensure exclusive access.
	// 2. We prioritize 'pending' messages but also recover 'processing' ones older than 5 min.
	// 3. The main UPDATE statement marks them as 'processing' and increments attempts.
	// 4. RETURNING returns the full rows, avoiding a second SELECT query.
	query := `
		WITH target_ids AS (
			SELECT id 
			FROM pg_sync_outbox 
			WHERE (
				(status = 'pending' AND attempts < 5)
				OR 
				(status = 'processing' AND updated_at < NOW() - INTERVAL '5 minutes' AND attempts < 5)
			)
			ORDER BY 
				CASE WHEN status = 'pending' THEN 0 ELSE 1 END,
				created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE pg_sync_outbox o
		SET status = 'processing',
		    attempts = o.attempts + 1,
		    updated_at = NOW()
		FROM target_ids
		WHERE o.id = target_ids.id
		RETURNING o.id, o.correlation_id, o.unit_id, o.table_name, o.operation, o.payload, o.attempts;
	`

	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to claim outbox batch: %v", err)
	}
	defer rows.Close()

	var entries []models.OutboxEntry
	for rows.Next() {
		var e models.OutboxEntry
		err := rows.Scan(
			&e.ID,
			&e.CorrelationID,
			&e.UnitID,
			&e.TableName,
			&e.Operation,
			&e.Payload,
			&e.Attempts,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan outbox entry: %v", err)
		}
		entries = append(entries, e)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit batch claim: %w", err)
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

func (r *PostgresRepository) MarkManyAsPending(ctx context.Context, ids []int64, reason string) error {
	query := `
        UPDATE pg_sync_outbox 
        SET status = 'pending', 
            attempts = attempts + 1, 
            error_log = $1, 
            updated_at = NOW() 
        WHERE id = ANY($2)`

	_, err := r.pool.Exec(ctx, query, "Batch Aborted: "+reason, ids)
	return err
}

// ResetStaleMessages returns messages that have been stuck in 'processing' to 'pending'
// for longer than the defined limit (timeoutMinutes)
func (r *PostgresRepository) ResetStaleMessages(ctx context.Context, timeoutMinutes int) (int64, error) {
	query := `
		UPDATE pg_sync_outbox 
		SET status = 'pending', 
		    updated_at = NOW(),
		    error_log = LEFT(COALESCE(error_log, '') || ' | [Janitor] Stuck recovery', 2000)
		WHERE status = 'processing' 
		  AND updated_at < NOW() - ($1 * INTERVAL '1 minute')`

	tag, err := r.pool.Exec(ctx, query, timeoutMinutes)
	if err != nil {
		return 0, fmt.Errorf("failed to reset stale messages: %w", err)
	}

	return tag.RowsAffected(), nil
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
