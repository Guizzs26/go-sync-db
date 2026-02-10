package service

import (
	"context"
	"fmt"
	"log"

	"github.com/Guizzs26/go-sync-db/internal/models"
)

type Repository interface {
	FetchPending(ctx context.Context, batchSize int) ([]models.OutboxEntry, error)
	MarkAsSent(ctx context.Context, id int64) error
	MarkAsError(ctx context.Context, id int64, errLog string) error
}

type BrokerClient interface {
	Publish(ctx context.Context, routingKey string, entry models.OutboxEntry) error
}

type SyncService struct {
	repo   Repository
	broker BrokerClient
}

func NewSyncService(r Repository, b BrokerClient) *SyncService {
	return &SyncService{
		repo:   r,
		broker: b,
	}
}

func (s *SyncService) ProcessNextBatch(ctx context.Context) error {
	entries, err := s.repo.FetchPending(ctx, 100)
	if err != nil {
		return fmt.Errorf("erro ao buscar pendências: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	for _, entry := range entries {
		routingKey := fmt.Sprintf("pax.unit.%d.%s.%s",
			entry.UnitID,
			entry.TableName,
			entry.Operation,
		)

		if err := s.broker.Publish(ctx, routingKey, entry); err != nil {
			s.repo.MarkAsError(ctx, entry.ID, err.Error())
			continue
		}

		if err = s.repo.MarkAsSent(ctx, entry.ID); err != nil {
			log.Printf("⚠️ Erro ao marcar como enviado ID %d: %v", entry.ID, err)
		}
	}

	return nil
}
