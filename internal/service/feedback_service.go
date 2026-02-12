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

// HandleDeadLetter processes a message retrieved from the .dead queue
func (s *FeedbackService) HandleDeadLetter(ctx context.Context, body []byte) error {
	var entry models.OutboxEntry
	if err := json.Unmarshal(body, &entry); err != nil {
		return err
	}

	s.logger.Warn("Feedback: moving record to error status in Sede", "correlation_id", entry.CorrelationID)

	// We mark as error so the Janitor can eventually move it to the physical pg_sync_dlq table
	return s.repo.MarkAsErrorByCorrelationID(ctx, entry.CorrelationID, "Fatal error reported by unit consumer")
}
