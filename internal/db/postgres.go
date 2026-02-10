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

func (r *PostgresRepository) FetchPending(ctx context.Context, batchSize int) ([]models.OutboxEntry, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	query := `
        SELECT id, correlation_id, unit_id, table_name, operation, payload, attempts
        FROM pg_sync_outbox
        WHERE status = 'pending'
        ORDER BY created_at ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    `

	rows, err := tx.Query(ctx, query, batchSize)
	if err != nil {
		return nil, fmt.Errorf("falha ao buscar pendências: %w", err)
	}
	defer rows.Close()

	var entries []models.OutboxEntry
	for rows.Next() {
		var entry models.OutboxEntry
		err := rows.Scan(
			&entry.ID,
			&entry.CorrelationID,
			&entry.UnitID,
			&entry.TableName,
			&entry.Operation,
			&entry.Payload,
			&entry.Attempts,
		)
		if err != nil {
			return nil, fmt.Errorf("erro no scan da outbox: %w", err)
		}
		entries = append(entries, entry)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("erro ao commitar transação: %w", err)
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
