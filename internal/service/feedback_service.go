package service

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/Guizzs26/go-sync-db/internal/models"
)

type FeedbackRepository interface {
	MarkAsErrorByCorrelationID(ctx context.Context, correlationID string, errLog string) error
}

type FeedbackService struct {
	repo   FeedbackRepository
	logger *slog.Logger
}

func NewFeedbackService(r FeedbackRepository, l *slog.Logger) *FeedbackService {
	return &FeedbackService{repo: r, logger: l}
}

func (s *FeedbackService) HandleDeadLetter(ctx context.Context, body []byte) error {
	var entry models.OutboxEntry
	if err := json.Unmarshal(body, &entry); err != nil {
		s.logger.Error("Feedback: failed to unmarshal dead letter", "error", err)
		return err
	}

	s.logger.Warn("Feedback: caught dead letter, updating database",
		"correlation_id", entry.CorrelationID,
		"table", entry.TableName)

	err := s.repo.MarkAsErrorByCorrelationID(ctx, entry.CorrelationID, "Fatal error reported by unit consumer")
	if err != nil {
		s.logger.Error("Feedback: failed to update postgres", "correlation_id", entry.CorrelationID, "error", err)
		return err
	}

	return nil
}
