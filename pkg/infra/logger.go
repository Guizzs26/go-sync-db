package infra

import (
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/Guizzs26/go-sync-db/internal/config"
)

func SetupLogger(cfg *config.Config) *slog.Logger {
	var level slog.Level
	switch strings.ToUpper(cfg.LogLevel) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logFile, _ := os.OpenFile("relay.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	multiWriter := io.MultiWriter(os.Stdout, logFile)

	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: level}

	if strings.ToUpper(cfg.LogFormat) == "JSON" {
		handler = slog.NewJSONHandler(multiWriter, opts)
	} else {
		handler = slog.NewTextHandler(multiWriter, opts)
	}

	return slog.New(handler)
}
