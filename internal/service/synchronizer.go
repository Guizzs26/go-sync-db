package service

import (
	"context"
	"fmt"
	"log"

	"github.com/Guizzs26/go-sync-db/internal/models"
)

type Repository interface {
	FetchAndClaim(ctx context.Context, batchSize int) ([]models.OutboxEntry, error)
	MarkAsSent(ctx context.Context, id int64) error
	MarkAsError(ctx context.Context, id int64, errLog string) error
	MarkAsPending(ctx context.Context, id int64, errLog string) error
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
	entries, err := s.repo.FetchAndClaim(ctx, 100)
	if err != nil {
		return fmt.Errorf("erro ao buscar pendências: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	for _, e := range entries {
		routingKey := fmt.Sprintf("pax.unit.%d.%s.%s",
			e.UnitID,
			e.TableName,
			e.Operation,
		)

		if err := s.broker.Publish(ctx, routingKey, e); err != nil {
			s.repo.MarkAsPending(ctx, e.ID, err.Error())
			continue
		}

		if err := s.repo.MarkAsSent(ctx, e.ID); err != nil {
			log.Printf("⚠️ Erro MarkAsSent [%s]: %v", e.CorrelationID, err)
		}
	}
	return nil
}
