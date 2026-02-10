package service

import (
	"context"
	"fmt"
	"log/slog"

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
	logger *slog.Logger
}

func NewSyncService(r Repository, b BrokerClient, l *slog.Logger) *SyncService {
	return &SyncService{
		repo:   r,
		broker: b,
		logger: l,
	}
}

func (s *SyncService) ProcessNextBatch(ctx context.Context) error {
	entries, err := s.repo.FetchAndClaim(ctx, 100)
	if err != nil {
		s.logger.Error("falha ao buscar pendências no banco", "error", err)
		return fmt.Errorf("erro ao buscar pendências: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	s.logger.Info("lote capturado para processamento", "count", len(entries))
	for _, e := range entries {
		l := s.logger.With(
			"correlation_id", e.CorrelationID,
			"unit_id", e.UnitID,
			"table", e.TableName,
		)

		l.Debug("processando mensagem")

		routingKey := fmt.Sprintf("pax.unit.%d.%s.%s",
			e.UnitID,
			e.TableName,
			e.Operation,
		)

		if err := s.broker.Publish(ctx, routingKey, e); err != nil {
			l.Error("falha na publicação no RabbitMQ", "error", err)
			s.repo.MarkAsPending(ctx, e.ID, err.Error())
			continue
		}

		if err := s.repo.MarkAsSent(ctx, e.ID); err != nil {
			l.Warn("mensagem enviada mas falhou ao marcar como 'sent'", "error", err)
		} else {
			l.Info("sincronização concluída com sucesso")
		}
	}

	return nil
}
