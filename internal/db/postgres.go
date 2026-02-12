package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

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

	// Immediate connectivity check with a strict 5s timeout
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := p.Ping(pingCtx); err != nil {
		return nil, fmt.Errorf("postgres ping failed: %v", err)
	}

	return &PostgresRepository{
		pool:   p,
		logger: logger,
	}, nil
}

// FetchAndClaim captures a batch of messages and marks them as 'processing' atomically
func (r *PostgresRepository) FetchAndClaim(ctx context.Context, batchSize int) ([]models.OutboxEntry, error) {
	opCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	tx, err := r.pool.Begin(opCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to start fetch transaction: %v", err)
	}
	defer tx.Rollback(opCtx)

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
				created_at ASC,
				id ASC 
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

	rows, err := tx.Query(opCtx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("claim batch query failed: %v", err)
	}
	defer rows.Close()

	var entries []models.OutboxEntry
	for rows.Next() {
		var e models.OutboxEntry
		err := rows.Scan(&e.ID, &e.CorrelationID, &e.UnitID, &e.TableName, &e.Operation, &e.Payload, &e.Attempts)
		if err != nil {
			return nil, fmt.Errorf("scan outbox entry failed: %v", err)
		}
		entries = append(entries, e)
	}

	if err := tx.Commit(opCtx); err != nil {
		return nil, fmt.Errorf("failed to commit batch claim: %v", err)
	}

	return entries, nil
}

// MarkAsSent uses a strict 5s timeout as this is a simple PK-based update
func (r *PostgresRepository) MarkAsSent(ctx context.Context, id int64) error {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `UPDATE pg_sync_outbox SET status = 'sent', updated_at = NOW() WHERE id = $1`
	if _, err := r.pool.Exec(opCtx, query, id); err != nil {
		return fmt.Errorf("failed to mark as sent (ID: %v): %v", id, err)
	}
	return nil
}

// MarkAsError persists the failure log. Attempts are not incremented here (already done in FetchAndClaim)
func (r *PostgresRepository) MarkAsError(ctx context.Context, id int64, errLog string) error {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `UPDATE pg_sync_outbox SET status = 'error', error_log = $2, updated_at = NOW() WHERE id = $1`
	_, err := r.pool.Exec(opCtx, query, id, errLog)
	return err
}

// MarkManyAsPending handles batch recovery with conditional attempt refund (StrategyInfraFailure)
func (r *PostgresRepository) MarkManyAsPending(ctx context.Context, ids []int64, note string, strategy models.RevertStrategy) error {
	if len(ids) == 0 {
		return nil
	}
	opCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	query := `
		UPDATE pg_sync_outbox 
		SET status = 'pending',
		    attempts = CASE WHEN $1 = TRUE THEN GREATEST(attempts - 1, 0) ELSE attempts END,
		    updated_at = NOW(),
		    error_log = LEFT(COALESCE(error_log, '') || $2, 2000)
		WHERE id = ANY($3)`

	_, err := r.pool.Exec(opCtx, query, strategy, " | "+note, ids)
	return err
}

// ResetStaleMessages is the Janitor's primary tool. Uses a 30s timeout for safety
func (r *PostgresRepository) ResetStaleMessages(ctx context.Context, timeoutMinutes int) (int64, error) {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	query := `
		UPDATE pg_sync_outbox 
		SET status = 'pending', 
		    updated_at = NOW(),
		    error_log = LEFT(COALESCE(error_log, '') || ' | [Janitor] Stuck recovery', 2000)
		WHERE status = 'processing' 
		  AND updated_at < NOW() - ($1 * INTERVAL '1 minute')`

	tag, err := r.pool.Exec(opCtx, query, timeoutMinutes)
	if err != nil {
		return 0, fmt.Errorf("failed to reset stale messages: %v", err)
	}

	return tag.RowsAffected(), nil
}

// MoveToDLQ migrates poison pills. Rationale: Uses SKIP LOCKED to avoid blocking the main Relay workers
func (r *PostgresRepository) MoveToDLQ(ctx context.Context) error {
	opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	query := `
        WITH poison_pills AS (
            SELECT id FROM pg_sync_outbox
            WHERE attempts >= 5
              AND status IN ('pending', 'error') 
              AND updated_at < NOW() - INTERVAL '10 minutes'
            FOR UPDATE SKIP LOCKED 
            LIMIT 100
        ),
        deleted AS (
            DELETE FROM pg_sync_outbox
            WHERE id IN (SELECT id FROM poison_pills)
            RETURNING *
        )
        INSERT INTO pg_sync_dlq (
            correlation_id, unit_id, table_name, operation, 
            payload, attempts, error_log, failed_at
        )
        SELECT 
            correlation_id, unit_id, table_name, operation,
            payload, attempts, error_log, NOW()
        FROM deleted
        RETURNING correlation_id;`

	rows, err := r.pool.Query(opCtx, query)
	if err != nil {
		return fmt.Errorf("DLQ migration failed: %v", err)
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err == nil {
			count++
			r.logger.Warn("Poison pill archived", "correlation_id", cid)
		}
	}

	if count > 0 {
		r.logger.Info("Maintenance: DLQ cycle finished", "moved", count)
	}
	return nil
}

func (r *PostgresRepository) Ping(ctx context.Context) error {
	opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	return r.pool.Ping(opCtx)
}

func (r *PostgresRepository) Close() {
	r.pool.Close()
}

// GetBacklogCount returns the number of messages waiting to be processed
func (r *PostgresRepository) GetBacklogCount(ctx context.Context) (int64, error) {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var count int64
	query := `SELECT COUNT(*) FROM pg_sync_outbox WHERE status IN ('pending', 'processing')`

	err := r.pool.QueryRow(opCtx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get backlog count: %w", err)
	}

	return count, nil
}

// GetDLQCount returns the number of messages in the Dead Letter Queue
func (r *PostgresRepository) GetDLQCount(ctx context.Context) (int64, error) {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var count int64
	query := `SELECT COUNT(*) FROM pg_sync_dlq`

	err := r.pool.QueryRow(opCtx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get DLQ count: %w", err)
	}

	return count, nil
}

func (r *PostgresRepository) MarkAsErrorByCorrelationID(ctx context.Context, correlationID string, errLog string) error {
	opCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		UPDATE pg_sync_outbox 
		SET status = 'error', 
		    error_log = LEFT(COALESCE(error_log, '') || ' | [Feedback] ' || $2, 2000), 
		    updated_at = NOW() 
		WHERE correlation_id = $1::uuid`

	_, err := r.pool.Exec(opCtx, query, correlationID, errLog)
	return err
}
