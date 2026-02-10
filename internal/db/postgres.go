package db

import (
	"context"
	"fmt"

	"github.com/Guizzs26/go-sync-db/internal/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(ctx context.Context, connString string) (*PostgresRepository, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("erro ao configurar pool do postgres: %w", err)
	}

	p, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("erro ao criar pool do postgres: %w", err)
	}

	if err := p.Ping(ctx); err != nil {
		return nil, fmt.Errorf("sem resposta do postgres: %w", err)
	}

	return &PostgresRepository{pool: p}, nil
}

func (r *PostgresRepository) FetchAndClaim(ctx context.Context, batchSize int) ([]models.OutboxEntry, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

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
        SELECT * FROM candidates FOR UPDATE SKIP LOCKED
    `

	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("falha ao buscar pendências: %w", err)
	}
	defer rows.Close()

	var entries []models.OutboxEntry
	var ids []int64

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
			return nil, fmt.Errorf("erro no scan da outbox: %w", err)
		}
		entries = append(entries, e)
		ids = append(ids, e.ID)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	if _, err = tx.Exec(ctx, `
        UPDATE pg_sync_outbox 
        SET status = 'processing',
            attempts = attempts + 1,
            updated_at = NOW()
        WHERE id = ANY($1)
    `, ids,
	); err != nil {
		return nil, fmt.Errorf("erro ao reivindicar mensagens: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("erro ao commitar: %w", err)
	}

	return entries, nil
}

func (r *PostgresRepository) MarkAsSent(ctx context.Context, id int64) error {
	query := `
		UPDATE pg_sync_outbox 
		SET status = 'sent', updated_at = CURRENT_TIMESTAMP 
		WHERE id = $1
	`
	_, err := r.pool.Exec(ctx, query, id)
	return err
}

func (r *PostgresRepository) MarkAsError(ctx context.Context, id int64, errLog string) error {
	query := `
		UPDATE pg_sync_outbox 
		SET status = 'error', 
		    attempts = attempts + 1, 
		    error_log = $2, 
		    updated_at = CURRENT_TIMESTAMP 
		WHERE id = $1
	`
	_, err := r.pool.Exec(ctx, query, id, errLog)
	return err
}

func (r *PostgresRepository) Close() {
	r.pool.Close()
}

func (r *PostgresRepository) MarkAsPending(ctx context.Context, id int64, errLog string) error {
	query := `
        UPDATE pg_sync_outbox 
        SET status = 'pending',
            error_log = $2,
            updated_at = CURRENT_TIMESTAMP 
        WHERE id = $1
    `
	_, err := r.pool.Exec(ctx, query, id, errLog)
	return err
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
        FROM moved
    `
	_, err := r.pool.Exec(ctx, query)
	return err
}
